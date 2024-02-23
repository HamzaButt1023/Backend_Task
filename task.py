import pandas as pd
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
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




model = SentenceTransformer('all-MiniLM-L6-v2')



def retrieve_answer(new_question: str):
    data = np.load('question_embeddings.npz')
    embeddings = data['embeddings']
    identifiers = data['identifiers']
    new_question_embedding = model.encode([new_question])
    similarities = cosine_similarity(new_question_embedding, embeddings)
    similarity_scores = similarities[0]
    max_similarity_index = similarity_scores.argmax()
    max_similarity = similarity_scores[max_similarity_index]
    if max_similarity > 0.52: 
        closest_question_identifier = identifiers[max_similarity_index]
        answer_doc = data_collection.find_one()
        for item in answer_doc['data']:
            if item["Question"] == closest_question_identifier:
                answer=item["Answer"]
                data={
                    "question":closest_question_identifier,
                    "answer":answer
                }
                return {"success":True,"message":"Question and Answer fetched successfully","data": data}
            else:
                answer=None
        data={
            "question":closest_question_identifier,
            "answer":answer
        }
        return {"success":True,"message":"Question and Answer fetched successfully","data": data}
    else:
        return {"success":True, "message":"No matching question and answer found"}



@app.get("/match_question")
async def retrieve_answer_endpoint(question: str):
    try:
        response = retrieve_answer(question)
        return JSONResponse(content=response, status_code=200)
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
        return {"success":False, "message":"Failed to insert data"},400

def save_embeddings(embeddings_file_path):
    df = pd.read_excel('Backend Developer assessment - Data.xlsx')
    question_embeddings = model.encode(df['Question'].tolist())
    question_identifiers = df['Question'].tolist()
    np.savez(embeddings_file_path, embeddings=np.array(question_embeddings), identifiers=np.array(question_identifiers))

def dump_data_in_db():
    df = pd.read_excel('Backend Developer assessment - Data.xlsx')
    qa_list = df.to_dict('records')
    data={
        "data":qa_list
    }
    data_collection.insert_one(data)
    return {"success":True,"message":"CSV Data dumped into MongoDB successfully"}
if __name__ == "__main__":
    embeddings_file_path = 'question_embeddings.npz'
    if not os.path.exists(embeddings_file_path):
        save_embeddings(embeddings_file_path)
        print("Data dumped in mongodb collection 'data' ")
    data=data_collection.find_one()
    if(not data):
        dump_data_in_db()
    uvicorn.run("task:app", port=3000, reload=True)