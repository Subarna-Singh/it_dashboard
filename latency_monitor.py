"""
Read Kafka Latencies
"""
import re
from asyncio import Queue, create_task, gather, get_event_loop, sleep
from collections import namedtuple
from dataclasses import dataclass

ServiceStatus = namedtuple(
    "ServiceStatus", ["service_name", "status", "metrics", "detail"]
)


class Monitor:
    """
    Monitor log file for status
    """

    @dataclass
    class Configuration:
        """
        Monitor Configuartion
        """

        component: str = "kafka"

        max_exchange_dataservice_latency_ms: float = 400
        """Maximum latency between the exchanges and the dataservice."""

        max_dataservice_broker_latency_ms: float = float("inf")
        """ Maximum latency between dataservice and broker"""

        max_broker_bot_latency_ms: float = float("inf")
        """ Maximum latency between broker and bot """

        logfile: str = "bot.log"
        """ log file to monitor """

    def __init__(self, config):
        self._config = config
        self.service_threshold = {
            "kafka_messages_received": None,
            "latency_exchange_to_dataservice": self._config.max_exchange_dataservice_latency_ms,
            "latency_dataservice_to_broker": self._config.max_dataservice_broker_latency_ms,
            "latency_broker_to_bot": self._config.max_broker_bot_latency_ms,
        }
        self.filename = self._config.logfile
        self.component = self._config.component
        self.message_queue = Queue()

    async def read_status(self):
        """
        read status updates from queue
        """
        while True:
            status = await self.message_queue.get()
            print(status)

    async def follow(self, file):
        """
        follow log file
        :returns: new line
        """
        file.seek(0, 2)  # Go to the end of the file
        while True:
            line = file.readline()
            if not line:
                await sleep(1)  # Sleep briefly
                continue
            yield line

    async def read_log(self):
        """
        read log file
        filter for component: kafka
        check status of component
        """
        logfile = open(self.filename)
        async for log in self.follow(logfile):
            if re.search(r"kafka", log):
                await sleep(0)
                self.check_status(log)

    def check_status(self, log):
        """
        check status and add status message to queue
        """
        regex = r"(\w+):\s(\d+)"
        result = re.findall(regex, log)
        for data in result:
            service_name = data[0]
            metric = float(data[1])
            self.message_queue.put_nowait(self.compose_status(service_name, metric))

    def compose_status(self, service_name: str, metric: float):
        """ 
        check service metric range
        compose status message
        """
        limit = self.service_threshold[service_name]
        status = "OK"
        status_detail = "None"

        if limit and metric > limit:
            status = "CRIT"
            status_detail = f"Latency at {metric} exceeded max limit {limit}"

        return ServiceStatus(service_name, status, metric, status_detail)


async def amain():
    """
    async main
    """
    monitor = Monitor(Monitor.Configuration)
    read_log = create_task(monitor.read_log())
    read_status = create_task(monitor.read_status())

    await gather(read_log, read_status)


def main():
    """
    main
    """
    loop = get_event_loop()
    loop.run_until_complete(amain())


# Example use
if __name__ == "__main__":
    main()
