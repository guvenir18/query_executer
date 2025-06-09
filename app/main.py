from fastapi import FastAPI

import frontend

app = FastAPI()

frontend.init(app)

if __name__ == "__main__":
    print('Please start the app with "uvicorn main:app --workers 1 --log-level info --port 8082" inside app directory')
