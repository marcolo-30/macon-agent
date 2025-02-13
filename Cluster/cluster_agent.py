import asyncio
import multiprocessing
import random
import spade
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour, OneShotBehaviour
from spade.template import Template
from spade.message import Message

from datetime import datetime, timedelta


class ClusterAgent(Agent):

    def __init__(self, jid: str, password: str):
        super().__init__(jid, password)
        self.check_process_behaviour = None
        self.launch_fluidity_behaviour = None
        self.analyze_behaviour = None
        self.ping_receiver_behaviour = None
        self.check_inactive_slaves_behaviour = None
        self.ping_behaviour = None
        self.deploy_behaviour = None
        self.monitor_behaviour = None
        self.lock = None
        self.subscribers = None
        self.timeout = timedelta(seconds=60)
        self.inform_config = None

    class ManageSubscriptionBehaviour(CyclicBehaviour):
        async def run(self):
            msg = await self.receive()  # Wait for a subscription message
            if msg:
                print(msg.thread)
                sender = str(msg._sender).split("/")[0]
                resp = Message(to=sender)  # Instantiate the message
                resp.set_metadata("performative", "subscription")
                resp.thread = msg.thread
                async with self.agent.lock:
                    if sender not in self.agent.subscribers:
                        self.agent.subscribers[sender] = True  # Assume true means available

                        resp.body = "Subscription succeed "  # Set the message content
                        await self.send(resp)
                    else:
                        print("JID on list already subscribed")
                        resp.body = "Agent already subscribed "  # Set the message content
                        await self.send(resp)

    class PingBehaviour(CyclicBehaviour):
        """
                A behavior that receives messages and sends responses.
                This is used to do the heartbeat.
         """

        async def run(self):
            """Continuously receive and respond to messages in a cyclic manner."""
            print("PingBehav running")

            # wait for a message for 10 seconds
            msg = await self.receive(timeout=10)
            if msg:
                print(str(msg._sender).split("/")[0])
                print(msg.to)
                print("Ping received with content: {}".format(msg.body))

                # Create a response message
                resp = Message(to=str(msg._sender).split("/")[0])  # Replace with the actual recipient JID
                resp.set_metadata("performative", "ping")  # Set the "inform" FIPA performative
                resp.body = "Response From " + str(msg.to)  # Set the message content
                print(resp.body)
                # Send the response message
                await self.send(resp)
                print("Callback message sent!\n")
            else:
                print("Did not received any message after 10 seconds")

                await asyncio.sleep(10)

    class PingReceiverBehaviour(CyclicBehaviour):
        async def run(self):
            msg = await self.receive(timeout=2)  # Wait for a message for 10 seconds
            if msg:
                slave_id = str(msg.sender).split("/")[0]
                now = datetime.now()
                self.agent.subscribers[slave_id] = now
                print(f"Received ping from {slave_id} at {now}.")

    class CheckInactiveSlavesBehaviour(PeriodicBehaviour):
        async def run(self):
            # Get the current time
            now = datetime.now()

            # Initialize an empty list to hold inactive subscribers
            inactive_slaves = []

            # Iterate through the subscribers dictionary
            for slave_id, last_ping in self.agent.subscribers.items():
                # Check if last_ping is a datetime object and if the time since last ping is greater than the timeout
                if isinstance(last_ping, datetime) and (
                        now - last_ping) > self.agent.timeout:  # time out is defined at the cluster agent
                    # Add the slave_id to the inactive list
                    inactive_slaves.append(slave_id)

            for slave_id in inactive_slaves:
                print("_____________________________________________INACTIVE NODE______________________")
                print(f"Agent {slave_id} is inactive.")
                print("_____________________________________________INACTIVE NODE______________________")

    async def setup(self):
        print("Starting cluster agent ... ")

        self.subscribers = {}
        self.lock = asyncio.Lock()

        # ---------------Manage subscription-----------------------

        sub_template = Template()
        sub_template.set_metadata("performative", "subscribe")
        agent_sub = self.ManageSubscriptionBehaviour()
        self.add_behaviour(agent_sub, sub_template)

        ping_template = Template()
        ping_template.set_metadata("performative", "ping")
        self.ping_behaviour = self.PingBehaviour()
        self.add_behaviour(self.ping_behaviour, ping_template)

        self.ping_receiver_behaviour = self.PingReceiverBehaviour()
        self.add_behaviour(self.ping_receiver_behaviour)

        self.check_inactive_slaves_behaviour = self.CheckInactiveSlavesBehaviour(period=5)
        self.add_behaviour(self.check_inactive_slaves_behaviour)


async def main():
    cluster_agent = ClusterAgent("cluster1@192.168.1.10", "1234")
    await cluster_agent.start(auto_register=True)
    print("Cluster Agent Started")

    await spade.wait_until_finished(cluster_agent)
    print("Agents finished")


if __name__ == "__main__":
    spade.run(main())
