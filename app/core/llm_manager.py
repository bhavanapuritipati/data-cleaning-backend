from typing import Any, Dict, List, Optional
from langchain_community.llms import HuggingFaceEndpoint
from app.config import settings

class LLMManager:
    """
    Manages interactions with HuggingFace models via LangChain or direct API.
    """
    
    def __init__(self, repo_id: str = "mistralai/Mistral-7B-Instruct-v0.2", temperature: float = 0.1):
        self.repo_id = repo_id
        self.temperature = temperature
        self.llm = self._initialize_llm()

    def _initialize_llm(self):
        """
        Initialize the LangChain HuggingFaceEndpoint LLM.
        """
        if not settings.HF_TOKEN:
            print("Warning: HF_TOKEN not set. Remote inference might fail.")
            
        return HuggingFaceEndpoint(
            repo_id=self.repo_id,
            huggingfacehub_api_token=settings.HF_TOKEN,
            temperature=self.temperature,
            max_new_tokens=512,
            top_p=0.95
        )

    def get_llm(self):
        return self.llm

    async def generate_response(self, prompt: str) -> str:
        """
        Generate a response from the LLM.
        """
        try:
            response = await self.llm.ainvoke(prompt)
            return response
        except Exception as e:
            print(f"Error calling LLM: {e}")
            return f"Error: {str(e)}"

# Singleton instance
llm_manager = LLMManager()
