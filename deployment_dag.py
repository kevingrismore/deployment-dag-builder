from prefect import flow, task, serve

from builder import DAGBuilder


@flow(log_prints=True, persist_result=True)
def flow_1(name: str):
    print(f"Hello {name} from deployment 1")
    return "result from deployment 1"


@flow(log_prints=True, persist_result=True)
def flow_2(name: str):
    print(f"Hello {name} from deployment 2")
    return "result from deployment 2"


@flow(log_prints=True)
def deployment_dag_flow():
    dag = DAGBuilder()

    task1 = dag.add_task("task1", return_something)
    task2 = dag.add_deployment(
        "task2", "flow-1", "deployment1", {"name": task1.result}, ["task1"]
    )
    dag.add_deployment("task3", "flow-2", "deployment2", {"name": "Kevin"}, ["task1"])
    dag.add_task("task4", print_something, {"input": task2.result}, ["task2", "task3"])

    dag.run()


@task
def return_something():
    return "Kevin"


@task
def print_something(input: str):
    print(input)


if __name__ == "__main__":
    serve(
        flow_1.to_deployment("deployment1"),
        flow_2.to_deployment("deployment2"),
        deployment_dag_flow.to_deployment("dag-example"),
    )
