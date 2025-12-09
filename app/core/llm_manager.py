from typing import Any, Dict, List, Optional
import httpx
from app.config import settings

class LLMManager:
    """
    Manages interactions with HuggingFace models via direct HTTP API (OpenAI-compatible).
    """
    
    def __init__(self, repo_id: str = "meta-llama/Meta-Llama-3-8B-Instruct", temperature: float = 0.1):
        self.repo_id = repo_id
        self.temperature = temperature
        self.api_url = "https://router.huggingface.co/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {settings.HF_TOKEN}",
            "Content-Type": "application/json"
        }

    async def generate_response(self, prompt: str) -> str:
        """
        Generate a response from the LLM using the OpenAI-compatible endpoint.
        """
        if not settings.HF_TOKEN:
            return "Error: HF_TOKEN is missing in .env"

        payload = {
            "model": self.repo_id,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 512,
            "temperature": self.temperature,
            "top_p": 0.95,
            "stream": False
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.api_url, 
                    headers=self.headers, 
                    json=payload, 
                    timeout=60.0 # generous timeout for inference
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # OpenAI format: choices[0].message.content
                    return data['choices'][0]['message']['content']
                else:
                    error_msg = f"Error {response.status_code}: {response.text}"
                    print(error_msg)
                    return error_msg

            except Exception as e:
                print(f"Error calling LLM: {e}")
                return f"Error: {str(e)}"

# Singleton instance
llm_manager = LLMManager()
