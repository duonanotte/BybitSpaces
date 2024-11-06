import asyncio
import os
import json
import aiofiles
import random
import brotli
import aiohttp
import traceback

from time import time
from urllib.parse import unquote, quote
from random import randint, choices, uniform
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw import types
from pyrogram.raw.types import InputBotAppShortName
from pyrogram.raw.functions.messages import RequestAppWebView
from typing import Tuple

from bot.config import settings
from bot.core.agents import generate_random_user_agent
from bot.utils.logger import logger
from bot.exceptions import InvalidSession
from bot.utils.connection_manager import manager, connection_manager
from .headers import headers

class Tapper:
    def __init__(self, tg_client: Client, proxy: str | None):
        self.tg_client = tg_client
        self.session_name = tg_client.name
        self.proxy = proxy

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}
        self.headers = headers.copy()

    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        if not settings.USE_PROXY:
            return True
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    async def get_tg_web_data(self) -> Tuple[str, str]:
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )

        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()
                except (Unauthorized, UserDeactivated, AuthKeyUnregistered) as e:
                    raise InvalidSession(self.session_name)

            while True:
                try:
                    peer = await self.tg_client.resolve_peer('bybit_spaces_bot')
                    break
                except FloodWait as fl:
                    fls = fl.value
                    logger.warning(f"{self.session_name} | FloodWait {fl} - Sleeping for {fls + 3} seconds")
                    await asyncio.sleep(fls + 3)

            refer_id = manager.get_ref_code(settings.REF_ID)
            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                app=InputBotAppShortName(bot_id=peer, short_name="SpaceS"),
                platform='android',
                write_allowed=True,
                start_param=refer_id
            ))

            auth_url = web_view.url
            tg_web_data = unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0])

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return tg_web_data, refer_id

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"<light-yellow>{self.session_name}</light-yellow> | Unknown error during Authorization: "
                         f"{error}")
            await asyncio.sleep(delay=3)

    async def login(self, http_client: aiohttp.ClientSession, refer_id: str):
        try:
            response = await http_client.post(
                "https://api2.bybit.com/web3/api/web3game/tg/registerOrLogin",
                json={"inviterCode": refer_id}
            )
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Login failed: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return None

            result = data.get("result", {})
            point = result.get("point", 0)

            logger.success(f"{self.session_name} | Login successfully!")
            logger.info(f"{self.session_name} | Current balance: <g>{point}</g> points")

            return data

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during login: {str(e)}")
            return None

    async def status(self, http_client: aiohttp.ClientSession):
        try:
            response = await http_client.post(
                "https://api2.bybit.com/web3/api/web3game/tg/user/farm/status"
            )
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Status check failed: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return None

            result = data.get("result", {})
            claim_point = result.get("claimPoint", 0)
            status = result.get("status")

            status_messages = {
                "FarmStatus_Wait_Claim": "reward ready to claim",
                "FarmStatus_Unspecified": "needs to be started",
                "FarmStatus_Running": "in progress"
            }

            status_message = status_messages.get(status, status)
            logger.info(
                f"{self.session_name} | Current farm status: <ly>{status_message}</ly> | Available points: <ly>{claim_point}</ly>")

            if status == "FarmStatus_Wait_Claim":
                claim_result = await self.claim_farm(http_client, claim_point)

                if claim_result and claim_result.get("retCode") == 0:
                    await asyncio.sleep(randint(2, 6))

                    farm_result = await self.start_farm(http_client)
                    if not farm_result or farm_result.get("retCode") != 0:
                        logger.error(f"{self.session_name} | Failed to start new farming session")
                else:
                    logger.error(f"{self.session_name} | Failed to claim farming rewards")

            elif status == "FarmStatus_Unspecified":
                await self.start_farm(http_client)

            return data

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during status check: {str(e)}")
            return None

    async def claim_farm(self, http_client: aiohttp.ClientSession, claim_point: int):
        try:
            response = await http_client.post(
                "https://api2.bybit.com/web3/api/web3game/tg/user/farm/claim"
            )
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Claim failed: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return None

            if response.status == 200:
                logger.success(f"{self.session_name} | Successfully claimed <ly>{claim_point}</ly> points!")

            return data

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during farm claim: {str(e)}")
            return None

    async def start_farm(self, http_client: aiohttp.ClientSession):
        try:
            response = await http_client.post(
                "https://api2.bybit.com/web3/api/web3game/tg/user/farm/start"
            )
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Failed to start farming: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return None

            if response.status == 200:
                logger.success(f"{self.session_name} | Starting new farm!")

            return data

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during farm start: {str(e)}")
            return None

    async def tasks(self, http_client: aiohttp.ClientSession):
        try:
            response = await http_client.get("https://api2.bybit.com/web3/api/web3game/tg/task/list")
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Failed to get task list: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return {"status": "error", "message": data.get('retMsg')}

            result = data.get("result", {})
            tasks = result.get("tasks", [])

            skip_task_ids = ['1', '4']
            filtered_tasks = [
                task for task in tasks
                if task.get("taskId") not in skip_task_ids
                   and task.get("status") == 1
            ]

            if not filtered_tasks:
                return {"status": "no_tasks"}

            completed_tasks = 0
            failed_tasks = 0
            total_points = 0

            for task in filtered_tasks:
                try:
                    task_id = task.get("taskId")
                    task_name = task.get("taskName")
                    points = int(task.get("point"))  # преобразуем строку в число

                    task_response = await http_client.post(
                        "https://api2.bybit.com/web3/api/web3game/tg/task/complete",
                        json={'taskId': task_id, 'tgVoucher': "todamoon"}
                    )
                    task_data = await task_response.json()
                    code = task_data.get("retCode")

                    if code == 0:
                        completed_tasks += 1
                        total_points += points
                        logger.success(
                            f"{self.session_name} | Completed task <ly>{task_name}</ly> | Received <ly>{points}</ly> points)"
                        )
                    else:
                        failed_tasks += 1
                        logger.warning(
                            f"{self.session_name} | Failed to complete task: {task_name} | Error: {task_data.get('retMsg')}"
                        )

                    await asyncio.sleep(randint(3, 15))

                except aiohttp.ClientError as e:
                    failed_tasks += 1
                    logger.error(f"{self.session_name} | Network error during task: {str(e)}")
                    continue

            return {"status": "success", "completed": completed_tasks, "failed": failed_tasks, "points": total_points}

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during tasks check: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def start_game(self, http_client: aiohttp.ClientSession):
        try:
            logger.info(f"{self.session_name} | Starting new game session...")

            response = await http_client.post(
                "https://api2.bybit.com/web3/api/web3game/tg/user/game/start"
            )
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Failed to start game: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return None

            if response.status == 200:
                logger.success(f"{self.session_name} | Game session started successfully!")
                if result := data.get("result"):
                    logger.info(f"{self.session_name} | Game session details: {result}")

            return data

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during game start: {str(e)}")
            return None

    async def finish_game(self, http_client: aiohttp.ClientSession):
        try:
            logger.info(f"{self.session_name} | Finishing game session...")

            response = await http_client.post(
                "https://api2.bybit.com/web3/api/web3game/tg/user/game/postScore"
            )
            response.raise_for_status()
            data = await response.json()

            if data.get("retCode") != 0:
                logger.error(f"{self.session_name} | Failed to finish game: {data.get('retMsg')}")
                logger.debug(f"{self.session_name} | Full response: {data}")
                return None

            if response.status == 200:
                logger.success(f"{self.session_name} | Game session completed successfully!")
                if result := data.get("result"):
                    if points := result.get("point"):
                        logger.info(f"{self.session_name} | Game rewards: {points} points")
                    logger.info(f"{self.session_name} | Game session details: {result}")

            return data

        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Network error during game finish: {str(e)}")
            return None

    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(
                f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        if settings.USE_PROXY:
            if not self.proxy:
                logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                return
            proxy_conn = ProxyConnector().from_url(self.proxy)
        else:
            proxy_conn = None

        http_client = aiohttp.ClientSession(headers=self.headers, connector=proxy_conn, trust_env=True)
        connection_manager.add(http_client)

        if settings.USE_PROXY:
            await self.check_proxy(http_client)

        token_live_time = randint(3500, 3600)
        tg_web_data, refer_id = await self.get_tg_web_data()
        access_token_created_time = 0
        while True:
            try:
                if http_client.closed:
                    if settings.USE_PROXY:
                        if proxy_conn and not proxy_conn.closed:
                            await proxy_conn.close()

                        if not self.proxy:
                            logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                            return
                        proxy_conn = ProxyConnector().from_url(self.proxy)
                    else:
                        proxy_conn = None

                    http_client = aiohttp.ClientSession(headers=self.headers, connector=proxy_conn, trust_env=True)
                    connection_manager.add(http_client)

                if time() - access_token_created_time >= token_live_time:
                    self.auth_token = tg_web_data
                    access_token_created_time = time()
                    token_live_time = randint(5000, 7000)

                http_client.headers['Authorization'] = f"{self.auth_token}"
                self.headers['Authorization'] = f"{self.auth_token}"

                login_result = await self.login(http_client, refer_id)
                if not login_result:
                    raise Exception("Login failed")

                await asyncio.sleep(randint(3, 15))

                status_result = await self.status(http_client)
                if not status_result:
                    raise Exception("Status check failed")

                if settings.AUTO_TASK:
                    tasks_result = await self.tasks(http_client)
                    if tasks_result:
                        if tasks_result["status"] == "error":
                            logger.warning(
                                f"{self.session_name} | Tasks processing failed: {tasks_result.get('message')}")
                        elif tasks_result["status"] == "no_tasks":
                            logger.info(f"{self.session_name} | No tasks available at the moment")
                        elif tasks_result["status"] == "success":
                            logger.info(f"{self.session_name} | Tasks completed: {tasks_result['completed']}, "
                                        f"Failed: {tasks_result['failed']}, "
                                        f"Total points earned: {tasks_result['points']}")


            except aiohttp.ClientConnectorError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ServerDisconnectedError as error:
                delay = random.randint(900, 1800)
                logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientResponseError as error:
                delay = random.randint(3600, 7200)
                logger.error(
                   f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientError as error:
                delay = random.randint(3600, 7200)
                logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except asyncio.TimeoutError:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except InvalidSession as error:
                logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                raise error


            except json.JSONDecodeError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            except KeyError as error:
                delay = random.randint(1800, 3600)
                logger.error(
                    f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except Exception as error:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Unexpected error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            finally:
                await http_client.close()
                if settings.USE_PROXY and proxy_conn and not proxy_conn.closed:
                    await proxy_conn.close()
                connection_manager.remove(http_client)

                sleep_time = random.randint(settings.SLEEP_TIME[0], settings.SLEEP_TIME[1])
                hours = int(sleep_time // 3600)
                minutes = (int(sleep_time % 3600)) // 60
                logger.info(
                    f"{self.session_name} | Sleep before wake up <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                await asyncio.sleep(sleep_time)


async def run_tapper(tg_client: Client, proxy: str | None):
    session_name = tg_client.name
    if settings.USE_PROXY and not proxy:
        logger.error(f"{session_name} | No proxy found for this session")
        return
    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{session_name} | Invalid Session")
