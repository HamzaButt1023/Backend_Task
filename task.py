import pandas as pd
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import JSONResponse
import asyncio
# import openai
from openai import OpenAI
from typing import List
load_dotenv()
app = FastAPI()
connection_string=os.environ.get("CONNECTION_STRING")
key=os.environ.get("OPENAI_API_KEY")
# Replace 'yourfile.csv' with the path to your CSV file
client = MongoClient(connection_string)
db = client['Task']  # Replace with your database name
data_collection = db['data']  # Replace with your collection name
inserted_data_collection = db['inserted_data']  # Replace with your collection name


async def main(new_question):
    data=data_collection.find_one()
    existing_questions = [item["Question"] for item in data['data']]
    # print(existing_questions)
    result = await match_question_and_retrieve_answer(new_question, existing_questions)
    print(result)
    return result

async def get_gpt_match(questions_chunk: List[str], new_question: str) -> str:
    try:
        model = "gpt-3.5-turbo"
        client = OpenAI()
        questions = '\n'.join(questions_chunk)
        prompt=f'''
        Given the question below:
        {new_question}

        I want you to return the question which is the most semantically similar and similiar in meaning and similar in what is asked, from this list of questions below
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
        # # Assuming `openai.Completion.create` is replaced with an async call
        # response = await openai.chatCompletion.create(
        #     engine="text-davinci-003",  # or whichever GPT-3.5 variant you're using
        #     temperature=0,
        #     max_tokens=60,
        #     top_p=1.0,
        #     frequency_penalty=0.0,
        #     presence_penalty=0.0,
        #     stop=["\n"]
        # )
        # return response.choices[0].text.strip()
    except Exception as e:
        print(f"Error in get_gpt_match: {e}")
        return ""

async def match_question_and_retrieve_answer(new_question: str, existing_questions: List[str]) -> dict:
    chunk_size = len(existing_questions) // 3
    question_chunks = [existing_questions[i:i + chunk_size] for i in range(0, len(existing_questions), chunk_size)]

    closest_questions = await asyncio.gather(*[get_gpt_match(chunk, new_question) for chunk in question_chunks])
    closest_questions = [i for i in closest_questions if i!="None"]
    if len(closest_questions) == 0:
        return {"No matching questions found"}
    else:
        while len(closest_questions)!=1:
            closest_questions = await asyncio.gather(*[get_gpt_match(chunk, new_question) for chunk in question_chunks])
            closest_questions = [i for i in closest_questions if i!="None"]
        result = closest_questions[0]
    # Retrieve the answer from MongoDB
        answer_doc = data_collection.find_one()
        for item in answer_doc['data']:
            if item["Question"] == result:
                answer=item["Answer"]
                return {"question": result, "answer":answer}
            else:
                answer=None
        
        return {"question": result, "answer":answer}
        

@app.post("/match_question")
async def match_question(new_question:str):
    # print(new_question,'question')
    result=await main(new_question)
    # print(xyz)
    return result


@app.get("/dump_data")
async def read_qa():
    df = pd.read_excel('Backend Developer assessment - Data.xlsx')
    qa_list = df.to_dict('records')
    data={
        "data":qa_list
    }
    response=data_collection.insert_one(data)
    return {"message":"CSV Data dumped into MongoDB successfully", "data":qa_list,"id":str(response.inserted_id)}

@app.post("/insert_data")
async def insert_data(question:str, answer:str):
    obj={"Question":question,"Answer":answer}
    data={
        "data":obj
    }
    response=inserted_data_collection.insert_one(data)
    return {"message":"Data dumped into MongoDB successfully", "data":obj,"id":str(response.inserted_id)}
    
