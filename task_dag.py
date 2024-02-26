from prefect import flow, task

from builder import DAGBuilder


@flow(log_prints=True)
def task_dag_flow():
    dag = DAGBuilder()

    dag.add_task("task1", print_something, {"input": "hello"}, [])
    dag.add_task("task2", add_something, {"num1": 1, "num2": 2}, ["task1"])
    dag_task_3 = dag.add_task("task3", return_something, {}, ["task1"])
    dag.add_task(
        "task4", print_something, {"input": dag_task_3.result}, ["task2", "task3"]
    )

    dag.run()

    print(dag_task_3.result.value)


@task
def print_something(input: str):
    print(input)


@task
def add_something(num1: int, num2: int):
    print(num1 + num2)


@task
def return_something():
    return "returned this string"


if __name__ == "__main__":
    task_dag_flow()
