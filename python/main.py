from ErrorRegister import ErrorsRegister


def main():

    ErrorsRegister.task_solve(
                    "../source/cheaters.db",
                    "../source/client.csv",
                    "../source/server.csv"
                    )


if __name__ == "__main__":
    main()
