import asyncio
import json
import multiprocessing
import random
import time

import yaml
from ruamel.yaml import YAML
import os
import spade
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour, OneShotBehaviour
from spade.template import Template
from spade.message import Message
import paramiko
import subprocess

command = "kubectl karmada --kubeconfig $HOME/.kube/karmada-apiserver.config apply -f ml-app.yaml"

from datetime import datetime, timedelta
from redis_setup import redis_mgt as rm

sleep_time = 10

# Define the remote server details
hostname = '10.96.12.152'
port = 22  # Default SSH port
username = 'runner'
password = 'csl123'  # It's better to use SSH keys for authentication if possible
remote_path = '/home/runner/ubiwhere-usecase/fluidity/resources/manifests/ml-app'
local_files = ['./ml-app.yaml', './ml-pod.yaml']

class ContinuumAgent(Agent):

    def __init__(self, jid: str, password: str):
        super().__init__(jid, password)

        self.is_subscribed = None
        self.ping_behaviour = None
        self.ping_receiver_behaviour = None
        self.check_inactive_slaves_behaviour = None
        self.update_data = None
        self.lock = None
        self.subscribers = None
        self.timeout = timedelta(seconds=60)
        self.app_memory = {}
        self.lock = asyncio.Lock()

    # ---------------------------------------- Subscription with cluster behaviours   ------------------------------------------

    class ManageSubscriptionBehaviour(CyclicBehaviour):
        async def run(self):

            msg = await self.receive()  # Wait for a subscription message
            if msg:
                print(msg.thread)
                sender = str(msg._sender).split("/")[0]
                resp = Message(to=sender)  # Instantiate the message
                resp.set_metadata("performative", "cluster_subs")
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

    # ---------------------------------------- API related behaviours   ------------------------------------------

    class APIPingBehaviour(CyclicBehaviour):
        """
                A behavior that receives messages and sends responses.
                This is used to do the heartbeat for API.
         """

        async def run(self):
            """Continuously receive and respond to messages in a cyclic manner."""
            print("APIPingBehav running")

            # wait for a message for 10 seconds
            msg = await self.receive(timeout=1)
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
                await asyncio.sleep(sleep_time)

   

    async def setup(self):
        print("Starting Continuum agent ... ")

        self.subscribers = {}
        self.lock = asyncio.Lock()
        self.is_subscribed = False

        redis_manager = rm.RedisManager()
        redis_manager.connect()

        sub_template = Template()
        sub_template.set_metadata("performative", "cluster_subs")
        agent_sub = self.ManageSubscriptionBehaviour()
        self.add_behaviour(agent_sub, sub_template)

        ping_template = Template()
        ping_template.set_metadata("performative", "ping")
        self.ping_behaviour = self.APIPingBehaviour()
        self.add_behaviour(self.ping_behaviour, ping_template)

        hb_template = Template()
        hb_template.set_metadata("performative", "clus_hb")
        self.ping_receiver_behaviour = self.PingReceiverBehaviour()
        self.add_behaviour(self.ping_receiver_behaviour)

        self.check_inactive_slaves_behaviour = self.CheckInactiveSlavesBehaviour(period=1)
        self.add_behaviour(self.check_inactive_slaves_behaviour)

      

async def main():
    continuum_agent = ContinuumAgent("continuum@192.168.1.10", "1234")
    await continuum_agent.start(auto_register=True)
    print("Continuum Agent Started.")

    await spade.wait_until_finished(continuum_agent)
    print("Agents finished")


if __name__ == "__main__":
    spade.run(main())
