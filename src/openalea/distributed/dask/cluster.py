from distributed import Client, LocalCluster


def main():

    cluster = LocalCluster(n_workers=4, scheduler_port=8786)
    client = Client(cluster)
    print client


if __name__ == "__main__":
    main()
