from prefect import flow, task, serve
from prefect.deployments import run_deployment
from prefect.client.schemas import FlowRun


@task
def run_deployment_task(name: str, parameters: dict):
    flow_run: FlowRun = run_deployment(name=name, parameters=parameters)
    return flow_run.state.result()


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
    future_1 = return_something.submit()
    future_2 = run_deployment_task.submit("flow-1/deployment1", {"name": future_1})
    future_3 = run_deployment_task.submit("flow-2/deployment2", {"name": future_1})
    print_something.submit(future_2, wait_for=[future_2, future_3])


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
