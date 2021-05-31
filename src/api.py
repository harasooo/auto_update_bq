from fastapi import FastAPI
from main import main


app = FastAPI()


@app.get("/ping")
def read_root():
    return {"status": "OK"}


@app.post("/scrape")
async def create_pred():
    main()
    return {"status": "OK"}
