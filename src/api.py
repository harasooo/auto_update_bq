from fastapi import FastAPI
from main import main


app = FastAPI()


@app.get("/")
def read_root():
    return {"status": "OK"}


@app.post("/")
async def create_pred():
    main()
    return {"status": "OK!"}
