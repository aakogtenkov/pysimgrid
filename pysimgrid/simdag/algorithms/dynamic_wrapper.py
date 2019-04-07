from .. import simulation as sim
from .. import scheduler
from ... import csimdag, cscheduling
import collections
import logging
import networkx
import os
import copy
import numpy as np
from collections import deque
import operator

def _all_parents_done_time(parents, tasks_status):
    result = 0.
    for parent, edge_dict in parents.items():
        parent_time = tasks_status[parent][2]
        if parent_time > result:
            result = parent_time
    return result

def _all_parents_done(parents, tasks_status):
    for parent, edge_dict in parents.items():
        if tasks_status[parent][0] != 'done':
            return False
    return True

def _all_parents_started(parents, tasks_status):
    for parent, edge_dict in parents.items():
        if tasks_status[parent][0] != 'done' and tasks_status[parent][0] != 'running':
            return False
    return True

def _all_parents_scheduled(parents, tasks_status):
    for parent, edge_dict in parents.items():
        if tasks_status[parent][0] != 'done' and tasks_status[parent][0] != 'running' and tasks_status[parent][0] != 'scheduled':
            return False
    return True

def _comm_time(host, parents, platform_model, tasks_status):
    result = 0.
    dst_idx = platform_model.host_idx(host)
    for parent, edge_dict in parents.items():
        src_idx = platform_model.host_idx(tasks_status[parent][1])
        if src_idx == dst_idx:
            parent_time = 0.
        else:
            comm_amount = edge_dict["weight"]
            parent_time = comm_amount / platform_model.bandwidth[src_idx, dst_idx] + platform_model.latency[src_idx, dst_idx]
        if parent_time > result:
            result = parent_time
    return result

def _get_done_tasks(schedule):
    done_tasks = set()
    for host in schedule.get_schedule().keys():
        for task, h, end_time in schedule.get_schedule()[host]:
            done_tasks.add(task)
    return done_tasks


class Schedule():
    def __init__(self, hosts):
        self._schedule = {h: [] for h in hosts}

    def timesheet_insertion_place(self, host, time_start, eet):
        timesheet = self._schedule[host] # (task, start_time, end_time)
        if len(timesheet) == 0:
            return 0, time_start, time_start + eet
        if timesheet[0][1] >= time_start + eet:
            return 0, time_start, time_start + eet
        for i in range(1, len(timesheet)):
            start_time = max(time_start, timesheet[i - 1][2])
            if start_time + eet <= timesheet[i][1]:
                return i, start_time, start_time + eet
        start_time = max(time_start, timesheet[-1][2])
        return len(timesheet), start_time, start_time + eet

    def insert_into_schedule(self, task, host, pos, time_start, time_end):
        self._schedule[host].insert(pos, (task, time_start, time_end))

    def _append(self, task, host, time_start, time_end):
        self._schedule[host].append((task, time_start, time_end))

    def get_schedule(self):
        return self._schedule


class StepTrigger():
    def __init__(self, steps=1):
        self.c = steps + 1
        self.steps = steps

    def trigger(self):
        self.c += 1
        if self.c > self.steps:
            self.c = 1
            return True
        return False


class SimpleSchedulePartitioner():
    def __init__(self):
        pass

    def freeze(self, schedule, tasks_status, hosts_status, task_graph):
        new_schedule = Schedule(schedule.get_schedule().keys())
        for host in schedule.get_schedule().keys():
            for task, start_time, end_time in schedule.get_schedule()[host]:
                status, h, ect = tasks_status[task]
                if status == 'running' or status == 'done':
                    new_schedule._append(task, host, -1, ect)
        return new_schedule

class ParallelSchedulePartitioner():
    def __init__(self):
        pass

    def freeze(self, schedule, tasks_status, hosts_status, task_graph, min_start_time=0):
        new_schedule = Schedule(schedule.get_schedule().keys())
        for host in schedule.get_schedule().keys():
            for task, start_time, end_time in schedule.get_schedule()[host]:
                status, h, ect = tasks_status[task]
                if status == 'running' or status == 'done' or start_time < min_start_time:
                    new_schedule._append(task, host, -1, ect)
        for host in schedule.get_schedule().keys():
            if cscheduling.is_master_host(host):
                pass
                # insert task for master node with complexity ~ scheduling time
                # (issue: need to schedule other part before it?)
                break
        return new_schedule


class DynamicWrapper(scheduler.DynamicScheduler):
    def __init__(self, simulation, static_scheduler, trigger, schedule_partitioner):
        super(DynamicWrapper, self).__init__(simulation)
        self.platform_model = cscheduling.PlatformModel(simulation)
        self.static_scheduler = static_scheduler
        self.trigger = trigger
        self.schedule_partitioner = schedule_partitioner
        self.task_graph = simulation.get_task_graph()
        self._schedule = Schedule(simulation.hosts)
        self.hosts_status = {h: True for h in simulation.hosts} # is free
        self.tasks_status = {t: ('schedulable', None, None) for t in simulation.tasks} # (flag, host, ect)

    def prepare(self, simulation):
        pass

    def schedule(self, simulation, changed):
        self.__update_host_status(changed)
        reschedule = self.trigger.trigger()
        if reschedule:
            self._schedule = self.schedule_partitioner.freeze(self._schedule, self.tasks_status, self.hosts_status, self.task_graph)
            self._schedule, expected_makespan = self.static_scheduler.get_schedule(self._schedule, 
                                                                                   self.task_graph, 
                                                                                   self.tasks_status)
        self.__schedule_to_free_hosts()

    def __update_host_status(self, changed):
        for t in changed.by_prop("kind", csimdag.TASK_KIND_COMM_E2E, True)[csimdag.TASK_STATE_DONE]:
            for h in t.hosts:
                self.hosts_status[h] = True
                #print('Task done', [_t for _t in self._simulation.tasks].index(t), self.tasks_status[t][2], self._simulation.clock)
                self.tasks_status[t] = ('done', h, self._simulation.clock)

    def __schedule_to_free_hosts(self):
        for host, tasks in self._schedule.get_schedule().items():
            if tasks and self.hosts_status[host]:
                for task, start_time, end_time in tasks:
                    if self.tasks_status[task][0] in ['done', 'running']:
                        continue
                    if _all_parents_done(dict(self.task_graph.pred[task]), self.tasks_status):
                        task.schedule(host)
                        self.hosts_status[host] = False
                        end_time = (self._simulation.clock + 
                                    self.platform_model.eet(task, host) + 
                                    _comm_time(host, dict(self.task_graph[task]), self.platform_model, self.tasks_status))
                        self.tasks_status[task] = ('running', host, end_time)
                        #print([_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host), self._simulation.clock)


class DynamicHEFT(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicHEFT, self).__init__(simulation)
        self.ordered_tasks = None

    def get_schedule(self, schedule, task_graph, tasks_status):

        platform_model = cscheduling.PlatformModel(self._simulation)
        state = cscheduling.SchedulerState(self._simulation)

        if self.ordered_tasks is None:
            self.ordered_tasks = cscheduling.heft_order(task_graph, platform_model)

        done_tasks = _get_done_tasks(schedule)

        expected_makespan = self._simulation.clock

        for task in self.ordered_tasks:
            if not task in done_tasks:
                best_eft = None
                host_to_schedule = None
                best_start = None
                best_pos = None
                for host in self._simulation.hosts:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue
                    parents_eft = _all_parents_done_time(dict(task_graph.pred[task]), tasks_status)
                    parents_eft = max(self._simulation.clock, parents_eft)
                    transmission_time = _comm_time(host, dict(task_graph.pred[task]), platform_model, tasks_status)
                    eet = platform_model.eet(task, host)

                    pos, time_start, eft = schedule.timesheet_insertion_place(host, parents_eft, transmission_time + eet)

                    if host_to_schedule is None or best_eft > eft:
                        best_eft = eft
                        host_to_schedule = host
                        best_start = time_start
                        best_pos = pos
                schedule.insert_into_schedule(task, host_to_schedule, best_pos, best_start, best_eft)
                tasks_status[task] = ('scheduled', host_to_schedule, best_eft)
                expected_makespan = max(expected_makespan, best_eft)
                
                #print([_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host_to_schedule), best_start, best_eft)
        #print()
        #print(expected_makespan)
        #for host in schedule:
        #    for task in schedule[host]:
        #        print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host))
        return schedule, expected_makespan

class DynamicHEFT_reschedule_inf(DynamicWrapper):
    def __init__(self, simulation):
        static_scheduler = DynamicHEFT(simulation)
        trigger = StepTrigger(1000000)
        schedule_partitioner = SimpleSchedulePartitioner()
        super(DynamicHEFT_reschedule_inf, self).__init__(simulation, static_scheduler, trigger, schedule_partitioner)

class DynamicHEFT_reschedule_1(DynamicWrapper):
    def __init__(self, simulation):
        static_scheduler = DynamicHEFT(simulation)
        trigger = StepTrigger(1)
        schedule_partitioner = SimpleSchedulePartitioner()
        super(DynamicHEFT_reschedule_1, self).__init__(simulation, static_scheduler, trigger, schedule_partitioner)

class DynamicHEFT_reschedule_5(DynamicWrapper):
    def __init__(self, simulation):
        static_scheduler = DynamicHEFT(simulation)
        trigger = StepTrigger(5)
        schedule_partitioner = SimpleSchedulePartitioner()
        super(DynamicHEFT_reschedule_5, self).__init__(simulation, static_scheduler, trigger, schedule_partitioner)


class DynamicLookahead(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicLookahead, self).__init__(simulation)
        self.ordered_tasks = None
        self.heft = DynamicHEFT(simulation)

    def _copy_schedule(self, schedule):
        schedule_copy = Schedule(schedule.get_schedule().keys())
        for h in schedule.get_schedule().keys():
            values = schedule.get_schedule()[h]
            for task, time_start, time_end in values:
                schedule_copy._append(task, h, time_start, time_end)
        return schedule_copy

    def _copy_tasks_status(self, tasks_status):
        tasks_status_copy = dict()
        for t in tasks_status.keys():
            tasks_status_copy[t] = tasks_status[t]
        return tasks_status_copy

    def get_schedule(self, schedule, task_graph, tasks_status):

        platform_model = cscheduling.PlatformModel(self._simulation)
        state = cscheduling.SchedulerState(self._simulation)

        if self.ordered_tasks is None:
            self.ordered_tasks = cscheduling.heft_order(task_graph, platform_model)

        done_tasks = _get_done_tasks(schedule)

        expected_makespan = self._simulation.clock

        for task in self.ordered_tasks:
            if not task in done_tasks:
                best_eft = None
                host_to_schedule = None
                best_start = None
                best_pos = None
                best_expected_makespan = None
                for host in self._simulation.hosts:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue

                    parents_eft = _all_parents_done_time(dict(task_graph.pred[task]), tasks_status)
                    parents_eft = max(self._simulation.clock, parents_eft)
                    transmission_time = _comm_time(host, dict(task_graph.pred[task]), platform_model, tasks_status)
                    eet = platform_model.eet(task, host)

                    pos, time_start, eft = schedule.timesheet_insertion_place(host, parents_eft, transmission_time + eet)

                    schedule_copy = self._copy_schedule(schedule)
                    tasks_status_copy = self._copy_tasks_status(tasks_status)

                    schedule_copy.insert_into_schedule(task, host, pos, time_start, eft)
                    tasks_status_copy[task] = ('scheduled', host, eft)

                    _, _expected_makespan = self.heft.get_schedule(schedule_copy, task_graph, tasks_status_copy)

                    #for task in tasks_status_copy:
                    #    print(tasks_status_copy[task], _expected_makespan)
                    #print(_expected_makespan, [_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host))

                    if host_to_schedule is None or _expected_makespan < best_expected_makespan:
                        best_eft = eft
                        host_to_schedule = host
                        best_start = time_start
                        best_pos = pos
                        best_expected_makespan = _expected_makespan

                schedule.insert_into_schedule(task, host_to_schedule, best_pos, best_start, best_eft)
                tasks_status[task] = ('scheduled', host_to_schedule, best_eft)
                expected_makespan = max(expected_makespan, best_eft)
                #print(best_eft, best_expected_makespan, [_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host_to_schedule))

                #print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host_to_schedule))
                #for parent, edge_dict in dict(nxgraph.pred[task]).items():
                #    print([_t for _t in simulation.tasks].index(parent), edge_dict['weight'], tasks_ect[parent])
                #print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host_to_schedule), best_start, best_eft, best_expected_makespan)
        #print()
        #print(expected_makespan)
        #for host in schedule:
        #    for task in schedule[host]:
        #        print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host))
        return schedule, expected_makespan

class DynamicLookahead_reschedule_inf(DynamicWrapper):
    def __init__(self, simulation):
        static_scheduler = DynamicLookahead(simulation)
        trigger = StepTrigger(10000000000000)
        schedule_partitioner = SimpleSchedulePartitioner()
        super(DynamicLookahead_reschedule_inf, self).__init__(simulation, static_scheduler, trigger, schedule_partitioner)


class DynamicPEFT(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicPEFT, self).__init__(simulation)
        self.ordered_tasks = None
        self.oct_dict = None

    def get_schedule(self, schedule, task_graph, tasks_status):
        platform_model = cscheduling.PlatformModel(self._simulation)

        if not self.ordered_tasks:
            self.oct_dict = self.build_oct_dict(task_graph, platform_model)
            oct_rank = {task: by_host.mean() for (task, by_host) in self.oct_dict.items()}
            self.ordered_tasks = cscheduling.schedulable_order(task_graph, oct_rank)

        done_tasks = _get_done_tasks(schedule)

        expected_makespan = self._simulation.clock

        for task in self.ordered_tasks:
            if not task in done_tasks:
                best_eft = None
                best_rank = None
                host_to_schedule = None
                best_start = None
                best_pos = None
                for host in self._simulation.hosts:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue
                    parents_eft = _all_parents_done_time(dict(task_graph.pred[task]), tasks_status)
                    parents_eft = max(self._simulation.clock, parents_eft)
                    transmission_time = _comm_time(host, dict(task_graph.pred[task]), platform_model, tasks_status)
                    eet = platform_model.eet(task, host)

                    pos, time_start, eft = schedule.timesheet_insertion_place(host, parents_eft, transmission_time + eet)

                    if host_to_schedule is None or best_rank > eft + self.oct_dict[task][platform_model.host_idx(host)]:
                        best_eft = eft
                        host_to_schedule = host
                        best_start = time_start
                        best_pos = pos
                        best_rank = eft + self.oct_dict[task][platform_model.host_idx(host)]

                schedule.insert_into_schedule(task, host_to_schedule, best_pos, best_start, best_eft)
                tasks_status[task] = ('scheduled', host_to_schedule, best_eft)
                expected_makespan = max(expected_makespan, best_eft)
                
                #print([_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host_to_schedule), best_start, best_eft)
        #print()
        #print(expected_makespan)
        #for host in schedule:
        #    for task in schedule[host]:
        #        print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host))
        return schedule, expected_makespan

    @classmethod
    def build_oct_dict(cls, nxgraph, platform_model):
        """
        Build optimistic cost table as an dict task->array.

        Args:
          nxgraph: networkx representation of task graph
          platform_model: platform linear model
        """
        result = dict()
        for task in list(reversed(list(networkx.topological_sort(nxgraph)))):
            oct_row = np.zeros(platform_model.host_count)
            if not nxgraph[task]:
                result[task] = oct_row
                continue
            for host, idx in platform_model.host_map.items():
                child_results = []
                for child, edge_dict in nxgraph[task].items():
                    row = result[child].copy()
                    row += child.amount / platform_model.speed
                    comm_cost = np.ones(platform_model.host_count) * edge_dict["weight"] / platform_model.mean_bandwidth
                    comm_cost += platform_model.mean_latency
                    comm_cost[idx] = 0
                    row += comm_cost
                    child_results.append(row.min())
                oct_row[idx] = max(child_results)
            result[task] = oct_row
        return result


class DynamicHCPT(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicHCPT, self).__init__(simulation)
        self.ordered_tasks = None

    def get_schedule(self, schedule, task_graph, tasks_status):

        platform_model = cscheduling.PlatformModel(self._simulation)
        state = cscheduling.SchedulerState(self._simulation)

        if self.ordered_tasks is None:
            self.ordered_tasks = self.order_tasks(task_graph, platform_model)

        done_tasks = _get_done_tasks(schedule)

        expected_makespan = self._simulation.clock

        for task in self.ordered_tasks:
            if not task in done_tasks:
                best_eft = None
                host_to_schedule = None
                best_start = None
                best_pos = None
                for host in self._simulation.hosts:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue
                    parents_eft = _all_parents_done_time(dict(task_graph.pred[task]), tasks_status)
                    parents_eft = max(self._simulation.clock, parents_eft)
                    transmission_time = _comm_time(host, dict(task_graph.pred[task]), platform_model, tasks_status)
                    eet = platform_model.eet(task, host)

                    pos, time_start, eft = schedule.timesheet_insertion_place(host, parents_eft, transmission_time + eet)

                    if host_to_schedule is None or best_eft > eft:
                        best_eft = eft
                        host_to_schedule = host
                        best_start = time_start
                        best_pos = pos
                schedule.insert_into_schedule(task, host_to_schedule, best_pos, best_start, best_eft)
                tasks_status[task] = ('scheduled', host_to_schedule, best_eft)
                expected_makespan = max(expected_makespan, best_eft)
                
                #print([_t for _t in self._simulation.tasks].index(task), [_h for _h in self._simulation.hosts].index(host_to_schedule), best_start, best_eft)
        #print()
        #print(expected_makespan)
        #for host in schedule:
        #    for task in schedule[host]:
        #        print([_t for _t in simulation.tasks].index(task), [_h for _h in simulation.hosts].index(host))
        return schedule, expected_makespan

    def order_tasks(self, task_graph, platform_model):
        tasks_aest, tasks_alst = self.get_tasks_aest_alst(task_graph, platform_model)

        # All nodes in the critical path must have AEST=ALST
        critical_path = sorted(
            [(t, tasks_aest[t]) for t in tasks_aest if np.isclose(tasks_aest[t], tasks_alst[t])],
            key=lambda x: x[1]
        )
        critical_path.reverse()

        stack = deque([elem[0] for elem in critical_path])
        ordered_tasks = []
        while len(stack):
            task = stack.pop()
            parents = task_graph.pred[task]
            untracked_parents = sorted(
                set(parents) - set(ordered_tasks),
                key=lambda x: tasks_aest[x]
            )
            if untracked_parents:
                stack.append(task)
                for parent in untracked_parents:
                    stack.append(parent)
            else:
                ordered_tasks.append(task)
        return ordered_tasks

    @classmethod
    def get_tasks_aest_alst(cls, nxgraph, platform_model):
        """
        Return AEST and ALST of tasks.

        Args:
          nxgraph: full task graph as networkx.DiGraph
          platform_model: cscheduling.PlatformModel object

        Returns:
          tuple containg 2 dictionaries
            aest: task->aest_value
            alst: task->alst_value
        """
        mean_speed = platform_model.mean_speed
        mean_bandwidth = platform_model.mean_bandwidth
        mean_latency = platform_model.mean_latency
        topological_order = list(networkx.topological_sort(nxgraph))

        # Average execution cost
        aec = {task: float(task.amount) / mean_speed for task in nxgraph}

        # Average earliest start time
        aest = {}
        # TODO: Check several roots and ends!
        root = topological_order[0]
        end = topological_order[-1]
        aest[root] = 0.
        for task in topological_order:
            parents = nxgraph.pred[task]
            if not parents:
                aest[task] = 0
                continue
            aest[task] = max([
                aest[parent] + aec[parent] + (nxgraph[parent][task]["weight"] / mean_bandwidth + mean_latency)
                for parent in parents
            ])

        topological_order.reverse()

        # Average latest start time
        alst = {}
        alst[end] = aest[end]
        for task in topological_order:
            if not nxgraph[task]:
                alst[task] = aest[task]
                continue
            alst[task] = min([
                alst[child] - (edge["weight"] / mean_bandwidth + mean_latency)
                for child, edge in nxgraph[task].items()
            ]) - aec[task]

        return aest, alst


class DynamicDLS(scheduler.StaticScheduler):
    def __init__(self, simulation):
        super(DynamicDLS, self).__init__(simulation)
        self.static_levels = None
        self.aec = None

    def get_schedule(self, schedule, task_graph, tasks_status):
        platform_model = cscheduling.PlatformModel(self._simulation)

        if self.aec is None or self.static_levels is None:
            self.aec, self.static_levels = self.get_tasks_sl_aec(task_graph, platform_model)

        done_tasks = _get_done_tasks(schedule)
        unreal_dl = 1 + max(self.static_levels.items(), key=operator.itemgetter(1))[1] + max(self.aec.items(), key=operator.itemgetter(1))[1]
        dl = {host: {task: unreal_dl for task in task_graph} for host in self._simulation.hosts}
        expected_makespan = self._simulation.clock

        queued_tasks = set()            # tasks with calculated DL
        waiting_tasks = set()           # tasks without calculated DL (their parents are not scheduled)
        for task in task_graph:
            if not task in done_tasks:
                tasks_status[task] = ('schedulable', None, None)
                if _all_parents_started(dict(task_graph.pred[task]), tasks_status):
                    for host in self._simulation.hosts:
                        dl[host][task] = self.calculate_dl(task_graph, tasks_status, platform_model, task, host, schedule)
                    queued_tasks.add(task)
                else:
                    waiting_tasks.add(task)

        while len(queued_tasks) > 0:
            cur_max = None
            task_to_schedule = None
            host_to_schedule = None
            best_start = None
            best_pos = None
            best_eft = None
            for host in self._simulation.hosts:
                for task in queued_tasks:
                    if task.name != 'root' and task.name != 'end' and cscheduling.is_master_host(host):
                        continue
                    if (task.name == 'root' or task.name == 'end') and not(cscheduling.is_master_host(host)):
                        continue
                    if cur_max is None or dl[host][task] > cur_max:
                        cur_max = dl[host][task]
                        host_to_schedule = host
                        task_to_schedule = task

                        # information for schedule update
                        parents_eft = _all_parents_done_time(dict(task_graph.pred[task]), tasks_status)
                        parents_eft = max(self._simulation.clock, parents_eft)
                        transmission_time = _comm_time(host, dict(task_graph.pred[task]), platform_model, tasks_status)
                        eet = platform_model.eet(task, host)
                        best_pos, best_start, best_eft = schedule.timesheet_insertion_place(host, parents_eft, transmission_time + eet)

            assert (cur_max is not None and host_to_schedule is not None)

            schedule.insert_into_schedule(task_to_schedule, host_to_schedule, best_pos, best_start, best_eft)
            tasks_status[task_to_schedule] = ('scheduled', host_to_schedule, best_eft)
            expected_makespan = max(expected_makespan, best_eft)

            queued_tasks.remove(task_to_schedule)

            for child, edge in task_graph[task_to_schedule].items():
                if _all_parents_scheduled(dict(task_graph.pred[child]), tasks_status):
                    waiting_tasks.remove(child)
                    queued_tasks.add(child)
                    for host in self._simulation.hosts:
                        dl[host][child] = self.calculate_dl(task_graph, tasks_status, platform_model, child, host, schedule)

            for task in queued_tasks:
                dl[host_to_schedule][task] = self.calculate_dl(task_graph, tasks_status, platform_model, task, host_to_schedule, schedule)

        #for host in self._simulation.hosts:
        #    print([_t for _t in self._simulation.hosts].index(host), end=': ')
        #    for task, _, _ in schedule._schedule[host]:
        #        print([_t for _t in self._simulation.tasks].index(task), end=', ')
        #    print()
        #print(expected_makespan)
        return schedule, expected_makespan

    def calculate_dl(self, task_graph, tasks_status, platform_model, task, host, schedule):
        parents_eft = _all_parents_done_time(dict(task_graph.pred[task]), tasks_status)
        parents_eft = max(self._simulation.clock, parents_eft)
        transmission_time = _comm_time(host, dict(task_graph.pred[task]), platform_model, tasks_status)
        eet = platform_model.eet(task, host)
        _, start, _ = schedule.timesheet_insertion_place(host, parents_eft, transmission_time + eet)
        return self.static_levels[task] + (self.aec[task] - task.amount / host.speed) - start

    def get_tasks_sl_aec(cls, task_graph, platform_model):
        """
        Return Average Execution Cost and Static Level for every task.

        Args:
          nxgraph: full task graph as networkx.DiGraph
          platform_model: cscheduling.PlatformModel object

        Returns:
            aec: task->aec_value
            sl: task->static_level_value
        """
        mean_speed = platform_model.mean_speed
        topological_order = list(reversed(list(networkx.topological_sort(task_graph))))

        # Average execution cost
        aec = {task: float(task.amount) / mean_speed for task in task_graph}

        sl = {task: aec[task] for task in task_graph}

        # Static Level
        for task in topological_order:
            for parent in task_graph.pred[task]:
                sl[parent] = max(sl[parent], sl[task] + aec[parent])

        return aec, sl