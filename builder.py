from collections import defaultdict, deque
from typing import Any, Callable

from prefect import task
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment
from prefect.utilities.collections import visit_collection

from pydantic import BaseModel, Field, root_validator


@task
def run_deployment_task(name: str, parameters: dict):
    flow_run: FlowRun = run_deployment(name=name, parameters=parameters)
    return flow_run.state.result()


class ResultPlaceholder(BaseModel):
    """Task result model"""

    task_name: str = Field(..., description="Task name")


class DAGTask(BaseModel):
    """Task model"""

    name: str = Field(..., description="Task name")
    task_function: Callable = Field(..., description="Task function")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Task parameters"
    )
    depends_on: list[str] = Field(
        default_factory=list, description="List of task names that this task depends on"
    )
    result: ResultPlaceholder = None

    @root_validator(pre=True)
    def prepare_result(cls, values):
        task_name = values.get("name")
        values["result"] = ResultPlaceholder(task_name=task_name)
        return values


class DAGBuilder(BaseModel):

    tasks: list[DAGTask] = Field(
        default_factory=list, description="Tasks to be executed"
    )
    futures: dict = Field(default_factory=dict)

    def _replace_result_placeholders(self, x):
        if isinstance(x, ResultPlaceholder):
            return self.futures[x.task_name].result()

        return x

    def add_task(
        self,
        name: str,
        task_function: Callable,
        parameters: dict = None,
        depends_on: list[str] = None,
    ):
        """Add a task to the DAG"""
        task = DAGTask(
            name=name,
            task_function=task_function,
            parameters={} if not parameters else parameters,
            depends_on=[] if not depends_on else depends_on,
        )
        self.tasks.append(task)
        return task

    def add_deployment(
        self,
        task_name: str,
        flow_name: str,
        deployment_name: str,
        parameters: dict,
        depends_on: list[str],
    ):
        return self.add_task(
            name=task_name,
            task_function=run_deployment_task,
            parameters={
                "name": f"{flow_name}/{deployment_name}",
                "parameters": parameters,
            },
            depends_on=depends_on,
        )

    def _find_concurrent_order(self):
        graph = defaultdict(list)  # Task -> List of tasks depending on it
        in_degree = defaultdict(int)  # Task -> Number of dependencies
        task_levels = defaultdict(
            set
        )  # Level -> Tasks that can be executed at this level

        # Build the graph and in-degree map
        for task in self.tasks:
            for dependency in task.depends_on:
                graph[dependency].append(task.name)
                in_degree[task.name] += 1

        # Initialize the queue with tasks having no dependencies
        queue = deque(
            [(task.name, 0) for task in self.tasks if in_degree[task.name] == 0]
        )

        while queue:
            task_name, level = queue.popleft()
            task_levels[level].add(task_name)

            for dependent in graph[task_name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append((dependent, level + 1))

        return task_levels

    def run(self):
        concurrent_order = self._find_concurrent_order()
        for level in sorted(concurrent_order.keys()):
            tasks_to_run: list[DAGTask] = [
                task for task in self.tasks if task.name in concurrent_order[level]
            ]
            for task in tasks_to_run:
                wait_for = [self.futures[dependency] for dependency in task.depends_on]

                task.parameters = visit_collection(
                    task.parameters, self._replace_result_placeholders, return_data=True
                )

                task_function = task.task_function.with_options(name=task.name)

                self.futures[task.name] = task_function.submit(
                    **task.parameters,
                    wait_for=wait_for,
                )
