import happybase


#Happy base e uma bibilioteca do python que facilita a integração como HBase
def connect_to_hbase():
    connection = happybase.Connection(host='localhost', port=9090)
    return connection


def create_table(connection):
    tables = connection.tables()
    if b'tasks' not in tables:
        connection.create_table(
            'tasks',
            {'task_info': dict(),}
        )


def add_task(connection, task_id, task_description):
    table = connection.table('tasks')
    table.put(task_id, {'task_info:description': task_description})


def delete_task(connection, task_id):
    table = connection.table('tasks')
    table.delete(task_id)


def list_tasks(connection):
    table = connection.table('tasks')
    tasks = {}
    for key, data in table.scan():
        task_description = data[b'task_info:description'].decode('utf-8')
        tasks[key.decode('utf-8')] = task_description
    return tasks


def main():
    connection = connect_to_hbase()
    create_table(connection)

    while True:
        print("\nGerenciador de Tarefas")
        print("1. Adicionar Tarefa")
        print("2. Excluir Tarefa")
        print("3. Listar Tarefas")
        print("4. Sair")

        choice = input("Escolha uma opção: ")

        if choice == '1':
            task_id = input("Informe o ID da tarefa: ")
            task_description = input("Informe a descrição da tarefa: ")
            add_task(connection, task_id, task_description)
            print("Tarefa adicionada com sucesso!")

        elif choice == '2':
            task_id = input("Informe o ID da tarefa a ser excluída: ")
            delete_task(connection, task_id)
            print("Tarefa excluída com sucesso!")

        elif choice == '3':
            tasks = list_tasks(connection)
            if not tasks:
                print("Nenhuma tarefa encontrada.")
            else:
                print("\nLista de Tarefas:")
                for task_id, task_description in tasks.items():
                    print(f"ID: {task_id}, Descrição: {task_description}")

        elif choice == '4':
            connection.close()
            print("Saindo do programa.")
            break

if __name__ == "__main__":
    main()
