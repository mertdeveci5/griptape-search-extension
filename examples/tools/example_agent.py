from griptape.structures import Agent
from griptape.apollo.tools.apollo import ApooloClient


agent = Agent(tools=[ApooloClient()])

agent.run("Find me account executives working in AI startups in new york")
