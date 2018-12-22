from .. import simulation
from .. import scheduler
from ... import csimdag
import collections
import logging
import networkx
import os

class DynamicSimulation(simulation.Simulation):
    def __init__(self, platform, tasks, config=None, log_config=None):
        super(DynamicSimulation, self).__init__(platform, tasks, config, log_config)
        self._done_tasks = set()

    def simulate(self, how_long=-1.):
        """
        Run the simgrid simulation until one of the following happens:
        
        * how_long time limit expires (if passed and positive)
        * watchpoint is reached (some task changed state)
        * simulation ends

        Returns the list of changed tasks.
        """
        changed = csimdag.simulate(how_long)
        changed_ids = [t.native for t in changed]
        done_tasks = self.tasks[csimdag.TaskState.TASK_STATE_DONE, 
                                csimdag.TaskState.TASK_STATE_RUNNING, 
                                csimdag.TaskState.TASK_STATE_FAILED, 
                                csimdag.TaskState.TASK_STATE_SCHEDULED]
        for task in done_tasks:
            self._done_tasks.add(task)
        return simulation._TaskList([t for t in self._tasks if t.native in changed_ids])

    def get_task_graph(self):
        """
        Get current DAG as a nxgraph.DiGraph.

        Task computation/communication amounts are represented as an "weight" attribute of nodes and edges.
        """
        free_tasks = self.tasks.by_func(lambda t: not t.parents)
        if len(free_tasks) != 1:
          raise Exception("cannot find DAG root")

        graph = networkx.DiGraph()
        for t in self.tasks:
            if not (t in self._done_tasks):
                graph.add_node(t, weight=t.amount)

        for e in self.connections:
            parents, children = e.parents, e.children
            assert len(parents) == 1 and len(children) == 1
            if not (parents[0] in self._done_tasks or children[0] in self._done_tasks):
                graph.add_edge(parents[0], children[0], weight=e.amount)

        return graph

class DynamicWrapper(scheduler.DynamicScheduler):
    def __init__(self, simulation, static_scheduler):
        assert type(simulation) == DynamicSimulation
        super(DynamicWrapper, self).__init__(simulation)
        self.static_scheduler = static_scheduler

    def prepare(self, simulation):
        self.hosts_status = {h: True for h in self._simulation.hosts}

    def schedule(self, simulation, changed):
        schedule, expected_makespan = self.static_scheduler.get_schedule(simulation)
        self.__update_host_status(self.hosts_status, changed)
        self.__schedule_to_free_hosts(schedule, self.hosts_status)

    def __update_host_status(self, hosts_status, changed):
        for t in changed.by_prop("kind", csimdag.TASK_KIND_COMM_E2E, True)[csimdag.TASK_STATE_DONE]:
            for h in t.hosts:
                hosts_status[h] = True

    def __schedule_to_free_hosts(self, schedule, hosts_status):
        for host, tasks in schedule.items():
            if tasks and hosts_status[host] == True:
                task = tasks.pop(0)
                task.schedule(host)
                hosts_status[host] = False