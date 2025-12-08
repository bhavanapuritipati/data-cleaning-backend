from fastapi import APIRouter, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
import shutil
import os
import uuid
import pandas as pd
import asyncio
from typing import Dict
from app.config import settings
from app.core.pipeline import pipeline
from app.core.state import PipelineState
from app.utils.file_handlers import save_upload_file, save_dataframe
from app.api.websocket import manager

router = APIRouter()

# In-memory storage for job status (in a real app, use Redis/Database)
jobs: Dict[str, Dict] = {}

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    job_id = str(uuid.uuid4())
    file_path = os.path.join(settings.UPLOAD_DIR, f"{job_id}.csv")
    
    try:
        await save_upload_file(file, file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save file: {str(e)}")
    
    jobs[job_id] = {
        "status": "uploaded",
        "file_path": file_path,
        "progress": 0
    }
    
    return {"job_id": job_id, "status": "uploaded"}

async def run_pipeline_task(job_id: str, file_path: str):
    # Initialize state
    try:
        df = pd.read_csv(file_path)
        initial_state: PipelineState = {
            "job_id": job_id,
            "original_df": df,
            "current_df": df,
            "schema_report": {},
            "imputation_report": {},
            "outlier_report": {},
            "transformation_report": {},
            "final_report": {},
            "current_agent": "schema_validator",
            "progress": 0,
            "errors": [],
            "status": "processing"
        }
        
        # Helper to broadcast updates
        # Since LangGraph is synchronous in structure but we want updates, 
        # we realistically need to inject a callback or handle state updates. 
        # For this simplified version, we will run the pipeline and updates happen within agents usually.
        # But agents don't have access to websocket manager.
        # Ideally, we pass a callback to agents or poll state.
        
        # Since our agents are async and return state, we can't easily stream intermediate updates 
        # unless we break down the execution or use LangGraph's streaming capabilities.
        # For simplicity, we will assume agents update global state or we just await the final result
        # and mock progress updates here for demonstration or re-architect slightly.
        
        # Better approach: We can invoke the graph and get the final state.
        # To get real-time updates, we would use `app.stream()` from LangGraph.
        
        jobs[job_id]["status"] = "processing"
        await manager.broadcast(job_id, {"status": "started", "progress": 0})
        
        # Async run
        final_state = await pipeline.ainvoke(initial_state)
        
        # Save output
        output_path = os.path.join(settings.OUTPUT_DIR, f"{job_id}_cleaned.csv")
        save_dataframe(final_state["current_df"], output_path)
        
        # Save report
        # In a real app we'd generate HTML. For now just JSON/Dict
        
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["final_state"] = final_state # Be careful with memory
        
        await manager.broadcast(job_id, {
            "status": "completed", 
            "progress": 100, 
            "report_summary": final_state.get("final_report", {}).get("summary")
        })
        
    except Exception as e:
        print(f"Pipeline error: {e}")
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        await manager.broadcast(job_id, {"status": "failed", "error": str(e)})


@router.post("/process/{job_id}")
async def start_processing(job_id: str, background_tasks: BackgroundTasks):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_info = jobs[job_id]
    if job_info["status"] != "uploaded":
        return {"message": "Job already processed or invalid status"}
    
    background_tasks.add_task(run_pipeline_task, job_id, job_info["file_path"])
    
    return {"job_id": job_id, "status": "processing_started"}

@router.get("/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Create a safe copy to return
    job_info = jobs[job_id].copy()
    
    # Remove non-serializable objects or large data
    if "final_state" in job_info:
        del job_info["final_state"]
        
    return job_info

@router.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await manager.connect(websocket, job_id)
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(job_id, websocket)

@router.get("/download/{job_id}/csv")
async def download_csv(job_id: str):
    file_path = os.path.join(settings.OUTPUT_DIR, f"{job_id}_cleaned.csv")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename="cleaned_data.csv")

@router.get("/download/{job_id}/report")
async def download_report(job_id: str):
    # Retrieve report from jobs or read saved file
    # For now returning a JSON response as we haven't implemented HTML gen
    if job_id not in jobs or "final_state" not in jobs[job_id]:
         raise HTTPException(status_code=404, detail="Report not ready")
    
    report = jobs[job_id]["final_state"].get("final_report")
    return JSONResponse(content=report)
