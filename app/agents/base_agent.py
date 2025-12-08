from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
from app.core.state import PipelineState

class BaseAgent(ABC):
    """
    Base class for all data cleaning agents.
    """
    
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def process(self, state: PipelineState) -> PipelineState:
        """
        Process the pipeline state and return the updated state.
        """
        pass

    def update_progress(self, state: PipelineState, progress: int, status: str = "processing") -> PipelineState:
        """
        Helper to update progress and current agent in state.
        """
        state["progress"] = progress
        state["current_agent"] = self.name
        state["status"] = status
        return state
