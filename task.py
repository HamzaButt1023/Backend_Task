import pandas as pd
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from fastapi import FastAPI
import asyncio
from openai import OpenAI
from typing import List
import uvicorn
from fastapi.responses import JSONResponse
load_dotenv()
app = FastAPI()
connection_string=os.environ.get("CONNECTION_STRING")
key=os.environ.get("OPENAI_API_KEY")
client = MongoClient(connection_string)
db = client['Task'] 
data_collection = db['data'] 
inserted_data_collection = db['inserted_data']





async def get_gpt_match(questions_chunk: List[str], new_question: str) -> str:
    try:
        model = "gpt-3.5-turbo"
        client = OpenAI()
        questions = '\n'.join(questions_chunk)
        prompt=f'''
        Given the question below:
        {new_question}

        I want you to return the question which is the most semantically and contextually similar and similiar in meaning from this list of questions below
        {questions}

        If no question from the list meets the matching criteria, return "None". Otherwise, return ONLY the matching question.
        '''
        response = client.chat.completions.create(
        model=model,
        messages=[
        {
        "role": "user",
        "content":prompt,
        }
            ]
        ,
        temperature=0.2,
        max_tokens=2000,
        )
        query_turbo_1=response.choices[0].message.content
        return query_turbo_1
    except Exception as e:
        print(str(e))
        return {"success":False, "message":"Failed to match question"}
async def match_question_and_retrieve_answer(new_question: str, existing_questions: List[str]) -> dict:
    chunk_size = len(existing_questions) // 3
    question_chunks = [existing_questions[i:i + chunk_size] for i in range(0, len(existing_questions), chunk_size)]
    closest_questions = await asyncio.gather(*[get_gpt_match(chunk, new_question) for chunk in question_chunks])
    closest_questions = [i for i in closest_questions if i!="None"]
    if len(closest_questions) == 0:
        return {"success":True,"message":"No matching question and answer found"}
    else:
        while len(closest_questions)!=1:
            closest_questions = await asyncio.gather(*[get_gpt_match(chunk, new_question) for chunk in question_chunks])
            closest_questions = [i for i in closest_questions if i!="None"]
        result = closest_questions[0]
        answer_doc = data_collection.find_one()
        for item in answer_doc['data']:
            if item["Question"] == result:
                answer=item["Answer"]
                data={
                    "question":result,
                    "answer":answer
                }
                return {"success":True,"message":"Question and Answer fetched successfully","data": data}
            else:
                answer=None
        return {"success":True,"message":"No matching question and answer found"}


        
async def main(new_question):
    data=data_collection.find_one()
    existing_questions = [item["Question"] for item in data['data']]
    result = await match_question_and_retrieve_answer(new_question, existing_questions)
    return result
@app.post("/match_question")
async def match_question(new_question:str):
    try:
        result=await main(new_question)
        return JSONResponse(content=result, status_code=200)
    except Exception as e:
        print(str(e))
        response={"success":False, "message":"Failed to match question"}
        return JSONResponse(content=response, status_code=500)



@app.post("/insert_data")
async def insert_data(question:str, answer:str):
    try:
        obj={"Question":question,"Answer":answer}
        data={
            "data":obj
        }
        response=inserted_data_collection.insert_one(data)
        return {"success":True,"message":"Data dumped into MongoDB successfully", "data":obj,"id":str(response.inserted_id)}
    except Exception as e:
        print(str(e))
        response={"success":False, "message":"Failed to insert data"}
        return JSONResponse(content=response, status_code=500)
def dump_data_in_db():
    df = pd.read_excel('Backend Developer assessment - Data.xlsx')
    qa_list = df.to_dict('records')
    data={
        "data":qa_list
    }
    data_collection.insert_one(data)
    return {"success":True,"message":"CSV Data dumped into MongoDB successfully"}
   
if __name__ == "__main__":
    data=data_collection.find_one()
    if(not data):
        dump_data_in_db()
        print("Data dumped in mongodb collection 'data' ")
    uvicorn.run("task:app", port=3000, reload=True)