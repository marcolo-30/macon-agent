import random
from queue import Queue
import spade
import os
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour, OneShotBehaviour
from spade.message import Message
from spade.template import Template

import config
import time
from datetime import datetime

window_time = 1


class NodeAgent(Agent):

    def __init__(self, jid: str, password: str, cluster_jid: str):
        super().__init__(jid, password)
        self.is_subscribed = None
        self.cluster = cluster_jid
        self.snapshot_queue = Queue()

    class Subscribe(CyclicBehaviour):
        def __init__(self, agent: str):
            super().__init__()

            self.agent_to_subscribe = agent

        async def run(self):
            print("Subscription behaviour running\n")
            msg = Message(to=str(self.agent_to_subscribe))  # Instantiate the message
            msg.set_metadata("performative", "subscribe")  # Set the "inform" FIPA performative
            msg.thread = str(random.randint(1000, 9999))
            # msg.set_metadata("body", "A")
            msg.body = "Subscribe request "  # Set the message content
            print(msg.thread)
            print("time sending the message ", datetime.now())
            await self.send(msg)
            print("time messahge sent ", datetime.now())
            # print("Subscription sent!\n")

            response = await self.receive(timeout=10)  # Wait for a response
            if response:
                print("response arrived at ", datetime.now())
                print(response.body)
                print("Subscription accepted, starting ping behaviour.\n")
                self.agent.is_subscribed = True
                self.kill()
            else:
                print("subscription failed")

    class HB_Behaviour(PeriodicBehaviour):
        async def run(self):
            print("Ping behaviour running\n")
            msg = Message(to=config.c_jid)  # Instantiate the message
            msg.set_metadata("performative", "inform")  # Set the "inform" FIPA performative
            msg.body = "Ping"  # Set the message content

            await self.send(msg)
            print("HB sent!\n")


    async def setup(self):
        print("Node Agent started ... ")
        self.is_subscribed = False

        agent_subscription_behaviour = self.Subscribe(self.cluster)
        self.add_behaviour(agent_subscription_behaviour)

        agent_subscription_behaviour2 = self.HB_Behaviour(period=20)
        self.add_behaviour(agent_subscription_behaviour2)



async def main():
    node_agent = NodeAgent(config.n_jid, config.n_pass, config.c_jid)
    await node_agent.start(auto_register=True)
    print("Node started")

    await spade.wait_until_finished(node_agent)
    print("Agents finished")


if __name__ == "__main__":
    spade.run(main())
