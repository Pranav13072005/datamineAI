from fastapi import APIRouter

from app.api.routes.health import router as health_router
from app.api.routes.datasets import router as datasets_router
from app.api.routes.export import router as export_router
from app.routes.query import router as query_router
from app.routes.upload import router as upload_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["Health"])
api_router.include_router(datasets_router, tags=["Datasets"], prefix="/datasets")

# Back-compat: legacy upload/query endpoints.
# We also mount upload under /datasets to satisfy the frontend.
api_router.include_router(upload_router, tags=["Datasets"], prefix="/datasets")
api_router.include_router(upload_router, tags=["Datasets"])  # POST /upload
api_router.include_router(query_router, tags=["Query"])      # POST /query, GET /history/{dataset_id}

api_router.include_router(export_router, tags=["Export"], prefix="/export")
