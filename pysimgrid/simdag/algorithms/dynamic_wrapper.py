from .. import simulation as sim
from .. import scheduler
from ... import csimdag, cscheduling
import collections
import logging
import networkx
import os
import copy

class DynamicSimulation(sim.Simulation):
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
        return sim._TaskList([t for t in self._tasks if t.native in changed_ids])

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


class DynamicWrapperV2(scheduler.DynamicScheduler):
    def __init__(self, simulation, static_scheduler, reschedule_frequency=1):
        super(DynamicWrapperV2, self).__init__(simulation)
        self.static_scheduler = static_scheduler
        self.done_tasks = set()
        self.tasks_ect = {task: 0 for task in simulation.tasks}
        self.task_host = {task: None for task in simulation.tasks}
        self._schedule = None
        self.reschedule_frequency = reschedule_frequency
        self.step_counter = reschedule_frequency

    def prepare(self, simulation):
        self.hosts_status = {h: True for h in self._simulation.hosts}
        self.hosts_eat = {h: [] for h in self._simulation.hosts} #(start_time, end_time) for tasks on host
        self._schedule = {h: [] for h in self._simulation.hosts}

    def schedule(self, simulation, changed):
        self.step_counter += 1
        if self.step_counter >= self.reschedule_frequency:
            self.step_counter = 0
            #drop old estimations
            for host in self.hosts_eat.keys():
                self.hosts_eat[host] = self.hosts_eat[host][:1]
                self._schedule[host] = self._schedule[host][:1]
            self._schedule, expected_makespan = self.static_scheduler.get_schedule(simulation, self.done_tasks, self.hosts_eat, self.task_host, self.tasks_ect, self._schedule)
        self.__update_host_status(self.hosts_status, changed)
        self.__schedule_to_free_hosts(self._schedule, self.hosts_status)

    def __update_host_status(self, hosts_status, changed):
        for t in changed.by_prop("kind", csimdag.TASK_KIND_COMM_E2E, True)[csimdag.TASK_STATE_DONE]:
            for h in t.hosts:
                self._schedule[h].remove(t)
                self.hosts_status[h] = True
                self.hosts_eat[h].pop(0)
                #print('Task done', [_t for _t in self._simulation.tasks].index(t), self.tasks_ect[t], self._simulation.clock)
                self.tasks_ect[t] = self._simulation.clock

    def __schedule_to_free_hosts(self, schedule, hosts_status):
        for host, tasks in schedule.items():
            if tasks and hosts_status[host] == True:
                task = tasks[0]
                task.schedule(host)
                hosts_status[host] = False
                self.done_tasks.add(task)
                self.hosts_eat[host][0] = (-1, self.hosts_eat[host][0][1]) #to prevent timesheet insertion before executing task
                #print([_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host), self._simulation.clock)


def _est(host, parents, platform_model, task_host, tasks_ect, hosts):
    result = 0.
    dst_idx = hosts.index(host)
    for parent, edge_dict in parents.items():
        src_idx = hosts.index(task_host[parent])
        if src_idx == dst_idx:
            parent_time = tasks_ect[parent]
        else:
            comm_amount = edge_dict["weight"]
            # extract ect first to ensure it has fixed type
            # otherwise + operator will trigger nasty python lookup
            parent_time = tasks_ect[parent]
            parent_time += comm_amount / platform_model.bandwidth[src_idx, dst_idx] + platform_model.latency[src_idx, dst_idx]
        if parent_time > result:
            result = parent_time
    return result

def _all_parents_done_time(parents, tasks_ect):
    result = 0.
    for parent, edge_dict in parents.items():
        parent_time = tasks_ect[parent]
        if parent_time > result:
            result = parent_time
    return result

def _comm_time(host, parents, platform_model, task_host, hosts):
    result = 0.
    dst_idx = hosts.index(host)
    for parent, edge_dict in parents.items():
        src_idx = hosts.index(task_host[parent])
        if src_idx == dst_idx:
            parent_time = 0.
        else:
            comm_amount = edge_dict["weight"]
            parent_time = comm_amount / platform_model.bandwidth[src_idx, dst_idx] + platform_model.latency[src_idx, dst_idx]
        if parent_time > result:
            result = parent_time
    return result

def timesheet_insertion_place(timesheet, time_start, eet):
    if len(timesheet) == 0:
        return 0, time_start, time_start + eet
    if timesheet[0][0] >= time_start + eet:
        return 0, time_start, time_start + eet
    for i in range(1, len(timesheet)):
        start_time = max(time_start, timesheet[i - 1][1])
        if start_time + eet <= timesheet[i][0]:
            return i, start_time, start_time + eet
    start_time = max(time_start, timesheet[-1][1])
    return len(timesheet), start_time, start_time + eet


class DynamicHEFT(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicHEFT, self).__init__(simulation)
        self.ordered_tasks = None

    def get_schedule(self, simulation, done_tasks, hosts_eat, task_host, tasks_ect, schedule, schedule_k=-1):
        """
        Overriden.
        """
        nxgraph = simulation.get_task_graph()
        platform_model = cscheduling.PlatformModel(simulation)
        state = cscheduling.SchedulerState(simulation)

        if self.ordered_tasks is None:
            self.ordered_tasks = cscheduling.heft_order(nxgraph, platform_model)

        if schedule_k < 0:
            schedule_k = len(self.ordered_tasks)

        #print(hosts_eat)
        #schedule = {host: [None] * len(hosts_eat[host]) for host in simulation.hosts}
        #for host in simulation.hosts:
        #    print(schedule[host], hosts_eat[host])
        #print(schedule)

        done_tasks2 = set()
        for h in schedule.keys():
            for task in schedule[h]:
                done_tasks2.add(task)
        done_tasks2 |= done_tasks

        for task in self.ordered_tasks:
            if not task in done_tasks2:
                best_eft = None
                host_to_schedule = None
                best_start = None
                best_pos = None
                for host in simulation.hosts:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue
                    """est1 = _all_parents_done_time(dict(nxgraph.pred[task]), tasks_ect)
                    est2 = _comm_time(host, dict(nxgraph.pred[task]), platform_model, task_host, [_host for _host in simulation.hosts])
                    time_start = est1 + est2
                    if len(hosts_eat[host]) > 0:
                        time_start = max(time_start, hosts_eat[host][-1])
                    time_start += est2"""
                    time_start = _est(host, dict(nxgraph.pred[task]), platform_model, task_host, tasks_ect, [_host for _host in simulation.hosts])
                    time_start = max(time_start, simulation.clock)
                    eet = platform_model.eet(task, host)

                    pos, time_start, eft = timesheet_insertion_place(hosts_eat[host], time_start, eet)

                    #if len(hosts_eat[host]) > 0:
                    #    time_start = max(time_start, hosts_eat[host][-1])

                    #eft = time_start + eet
                    if host_to_schedule is None or best_eft > eft:
                        best_eft = eft
                        host_to_schedule = host
                        best_start = time_start
                        best_pos = pos
                schedule[host_to_schedule].insert(best_pos, task)
                task_host[task] = host_to_schedule
                tasks_ect[task] = best_eft
                hosts_eat[host_to_schedule].insert(best_pos, (best_start, best_eft))
                #hosts_eat[host_to_schedule].append(best_eft)

                #print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host_to_schedule))
                #for parent, edge_dict in dict(nxgraph.pred[task]).items():
                #    print([_t for _t in simulation.tasks].index(parent), edge_dict['weight'], tasks_ect[parent])
                #print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host_to_schedule), best_start, best_eft)
        expected_makespan = max(tasks_ect.values())
        #print()
        #print(expected_makespan)
        #for host in schedule:
        #    for task in schedule[host]:
        #        print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host))
        return schedule, expected_makespan

class DynamicLookahead(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicLookahead, self).__init__(simulation)
        self.ordered_tasks = None
        self.heft = DynamicHEFT(simulation)

    def _copy_schedule(self, schedule):
        schedule_copy = dict()
        for h in schedule.keys():
            schedule_copy[h] = []
            for i in range(len(schedule[h])):
                schedule_copy[h].append(schedule[h][i])
        return schedule_copy

    def _copy_task_host(self, task_host):
        task_host_copy = dict()
        for h in task_host.keys():
            task_host_copy[h] = task_host[h]
        return task_host_copy

    def get_schedule(self, simulation, done_tasks, hosts_eat, task_host, tasks_ect, schedule, schedule_k=-1):
        nxgraph = simulation.get_task_graph()
        platform_model = cscheduling.PlatformModel(simulation)
        state = cscheduling.SchedulerState(simulation)

        if self.ordered_tasks is None:
            self.ordered_tasks = cscheduling.heft_order(nxgraph, platform_model)

        if schedule_k < 0:
            schedule_k = len(self.ordered_tasks)

        done_tasks2 = set()
        for h in schedule.keys():
            for task in schedule[h]:
                done_tasks2.add(task)
        done_tasks2 |= done_tasks

        for task in self.ordered_tasks:
            if not task in done_tasks2:
                best_expected_makespan = None
                best_eft = None
                host_to_schedule = None
                best_start = None
                best_pos = None
                for host in simulation.hosts:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue

                    time_start = _est(host, dict(nxgraph.pred[task]), platform_model, task_host, tasks_ect, [_host for _host in simulation.hosts])
                    time_start = max(time_start, simulation.clock)
                    eet = platform_model.eet(task, host)

                    pos, time_start, eft = timesheet_insertion_place(hosts_eat[host], time_start, eet)

                    hosts_eat[host].insert(pos, (time_start, eft))
                    tasks_ect[task] = eft
                    task_host[task] = host
                    schedule[host].insert(pos, task)

                    _, expected_makespan = self.heft.get_schedule(simulation, done_tasks, 
                                                                  self._copy_schedule(hosts_eat), 
                                                                  self._copy_task_host(task_host), 
                                                                  self._copy_task_host(tasks_ect), 
                                                                  self._copy_schedule(schedule))
                    #print(expected_makespan, [_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host), schedule, hosts_eat)

                    if host_to_schedule is None or expected_makespan < best_expected_makespan:
                        best_eft = eft
                        host_to_schedule = host
                        best_start = time_start
                        best_pos = pos
                        best_expected_makespan = expected_makespan

                    hosts_eat[host].pop(pos)
                    tasks_ect[task] = 0
                    task_host[task] = None
                    schedule[host].pop(pos)

                schedule[host_to_schedule].insert(best_pos, task)
                task_host[task] = host_to_schedule
                tasks_ect[task] = best_eft
                hosts_eat[host_to_schedule].insert(best_pos, (best_start, best_eft))

                #print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host_to_schedule))
                #for parent, edge_dict in dict(nxgraph.pred[task]).items():
                #    print([_t for _t in simulation.tasks].index(parent), edge_dict['weight'], tasks_ect[parent])
                #print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host_to_schedule), best_start, best_eft, best_expected_makespan)
        expected_makespan = max(tasks_ect.values())
        #print()
        #print(expected_makespan)
        #for host in schedule:
        #    for task in schedule[host]:
        #        print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host))
        return schedule, expected_makespan