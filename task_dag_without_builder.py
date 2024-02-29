from prefect import flow, task


@flow(log_prints=True)
def task_dag_flow():
    future_1 = print_something.submit("hello")
    future_2 = add_something.submit(1, 2, wait_for=[future_1])
    future_3 = return_something.submit(wait_for=[future_1])
    print_something.submit(future_3, wait_for=[future_2, future_3])


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
