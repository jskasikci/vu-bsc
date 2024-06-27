import subprocess
import csv

create_workflow_command = ["python3", "create_workflow.py"]

commands = [
    (["java", "-jar", "openDC.jar", "--scenario-path", "scenarios/simple_scenario.json", "-p", "1", "--scheduler", "fcfs"], "fcfs"),
    (["java", "-jar", "openDC.jar", "--scenario-path", "scenarios/simple_scenario.json", "-p", "1", "--scheduler", "sjf"], "sjf"),
    (["java", "-jar", "openDC.jar", "--scenario-path", "scenarios/simple_scenario.json", "-p", "1", "--scheduler", "ljf"], "ljf"),
    (["java", "-jar", "openDC.jar", "--scenario-path", "scenarios/simple_scenario.json", "-p", "1", "--scheduler", "heft"], "heft")
]

makespan_command = ["python3", "makespan.py"]

csv_file_path = "makespans.csv"

try:
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([name for _, name in commands])

        for i in range(100):
            print(f"Iteration {i+1}\n")

            create_dag_result = subprocess.run(create_workflow_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            print("Workflow creator script output:")
            print(create_dag_result.stdout)

            makespan_results = []

            for command, name in commands:
                result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                print(f"Command output ({name} scheduler):")
                print(result.stdout)

                makespan_result = subprocess.run(makespan_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                makespan_output = makespan_result.stdout.strip()
                print(f"Makespan output after {name} scheduler:")
                print(makespan_output + "\n")

                makespan_results.append(makespan_output)

            writer.writerow(makespan_results)

            print("\n" + "="*80 + "\n")

    print(f"Makespans have been written to {csv_file_path}")

except subprocess.CalledProcessError as e:
    print(f"An error occurred: {e}")
    print(f"Command output: {e.output}")
    print(f"Command stderr: {e.stderr}")
