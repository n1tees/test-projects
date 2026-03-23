from fastapi import APIRouter

from backend.interfaces.http.public_report import router as public_report_router

api_router = APIRouter()
api_router.include_router(public_report_router)
