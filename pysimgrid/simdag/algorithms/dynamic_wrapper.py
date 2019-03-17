from .. import simulation as sim
from .. import scheduler
from ... import csimdag, cscheduling
import collections
import logging
import networkx
import os
import copy

"""
def _est(host, parents, platform_model, tasks_status):
    result = 0.
    dst_idx = platform_model.host_idx(host)
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
"""

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
        trigger = StepTrigger(1)
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