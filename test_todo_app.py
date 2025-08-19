from fastapi import FastAPI
from enum import IntEnum
from typing import List, Optional
from pydantic import BaseModel, Field
from fastapi import HTTPException
import time

app = FastAPI()


class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class TodoBase(BaseModel):
    todo_name: str = Field(
        ..., min_length=3, max_length=512, description="Name of the task"
    )
    todo_desc: str = Field(..., description="Description of the task")
    priority: Priority = Field(default=Priority.LOW, description="Priority of the task")


class TodoCreate(TodoBase):
    pass


class Todo(TodoBase):
    todo_id: int = Field(..., description="Unique identifier of task")


class TodoUpdate(BaseModel):
    todo_name: Optional[str] = Field(
        None, min_length=3, max_length=512, description="Name of the task"
    )
    todo_desc: Optional[str] = Field(None, description="Description of the task")
    priority: Optional[Priority] = Field(None, description="Priority of the task")


all_todos = [
    Todo(
        todo_id=1,
        todo_name="Buy groceries",
        todo_desc="Milk, Bread, Eggs",
        priority=Priority.LOW,
    ),
    Todo(
        todo_id=2,
        todo_name="Walk the dog",
        todo_desc="Take the dog for a walk in the park",
        priority=Priority.MEDIUM,
    ),
    Todo(
        todo_id=3,
        todo_name="Read a book",
        todo_desc="Finish reading 'The Great Gatsby'",
        priority=Priority.HIGH,
    ),
]


@app.get("/todos", response_model=List[Todo])
def get_todos(first_n: int = None):
    if first_n:
        return all_todos[: min(first_n, len(all_todos))]
    return all_todos


@app.get("/todos/{todo_id}", response_model=Todo)
def get_todo(todo_id: int):
    for todo in all_todos:
        if todo.todo_id == todo_id:
            return todo
    raise HTTPException(status_code=404, detail="Todo not Found")


@app.post("/todos", response_model=Todo)
def create_todo(todo: TodoCreate):
    new_id = max(i.todo_id for i in all_todos) + 1
    new_todo = Todo(
        todo_id=new_id,
        todo_name=todo.todo_name,
        todo_desc=todo.todo_desc,
        priority=todo.priority,
    )
    all_todos.append(new_todo)
    return new_todo


@app.put("/todos/{todo_id}", response_model=Todo)
def update_todo(todo_id: int, update_todo: TodoUpdate):
    for todo in all_todos:
        if todo_id == todo.todo_id:
            if update_todo.todo_name:
                todo.todo_name = update_todo.todo_name
            if update_todo.todo_desc:
                todo.todo_desc = update_todo.todo_desc
            if update_todo.priority:
                todo.priority = update_todo.priority
            return todo
    raise HTTPException(status_code=404, detail="Todo not Found")


@app.delete("/todos/{todo_id}", response_model=Todo)
def delete_todo(todo_id: int):
    for idx, todo in enumerate(all_todos):
        if todo_id == todo.todo_id:
            deleted_todo = all_todos.pop(idx)
            return deleted_todo
    raise HTTPException(status_code=404, detail="Todo not Found")
