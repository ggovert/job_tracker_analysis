import requests
import json
import sys
from openai import OpenAI
import os


def test_ai_comment_extraction(job_comment):
    # 1. Configuration (matches your setup)
    local_url = "http://localhost:11434/v1"

    client = OpenAI(
        base_url=local_url,
        api_key="none", # Local engines don't check this, but the SDK needs a string
    )
    
    # Example comment from a job site
    # job_comment = """
    #     Gambit Robotics | Founding Principal / Staff Full-Stack Engineer | NYC | Onsite | Full-time
    # Hi! I’m Nicole (ex-Google AI), cofounder of Gambit Robotics with Eliot Horowitz (cofounder/CTO MongoDB).

    # We’re hiring for two founding engineering roles to work directly with us in our NYC office (in-person only):

    # 1) Founding Principal / Staff Full-Stack Engineer - 8+ years experience shipping production systems - Strong backend and systems foundation - Experience owning architecture end-to-end (zero → production)

    # 2) Founding Full-Stack Engineer - 4–6 years experience building and shipping production products - Strong product sense and ownership - Comfortable working across backend, client, and infrastructure

    # https://www.gambitrobotics.ai/careers

    # Gambit is an AI-powered kitchen assistant that mounts magnetically under most range hoods or microwaves. It uses computer vision, thermal imaging, and on-device AI to understand what’s happening in the pan in real time, and handles the hardest parts of cooking — heat, timing, and doneness — so people get better results with less stress.

    # We’re moving quickly from prototype to product and launching via Kickstarter later this month. It’s a seed-stage startup with high autonomy, real impact, and a very intense (and fun) next phase.

    # If you or someone you know might be a fit, email me: nicole@gambitrobotics.com

    # https://www.gambitrobotics.ai
    # """
    
    system_prompt = """
    You are a job , role, location, tech stack data extractor. Your output MUST be ONLY a valid JSON object.
    Extract ONLY roles and tech mentioned EXPLICITLY. Do not infer or guess.

    Schema:
    {"roles": ["string"], "tech_stack": ["string"], "location": "string or null", "company": "string or null"}

    EXAMPLES:
        Input: "Hiring a Java Backend Engineer. Remote."
        Output: {"roles": ["Java Backend Engineer"], "roles_category": ["Backend"], "tech_stack": ["Java"], "location": "Remote", "company": null}

        Input: "Tech company looking for someone with Python skills."
        Output: {"roles": [], "tech_stack": ["Python"], "location": null, "company": "Tech company"}

    RULES:
    - If a specific role title is not found, leave "roles" as [].
    - DO NOT add "Data Scientist" or "Game Developer" unless those exact words appear.
    - NO introductory text. NO markdown.
    """
    

    print(f"📡 Sending request to {local_url}...")
    print(f"💬 Input Comment: '{job_comment}'\n")
    print("🤖 AI Response: ", end="", flush=True)

    try:
        # 3. Make the streaming request
        response = client.chat.completions.create(
            model= "smollm2",
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": job_comment}
            ]
        )
        # Handle the stream...
        print(response.choices[0].message.content)

    except Exception as e:
        print(f"\n❌ Error connecting to AI: {e}")

if __name__ == "__main__":
    with open("scripts/jobs_sample.json", "r") as f:
        job_comment = json.load(f)
    job_spec = [text["text"] for text in job_comment[:10]]
    for i, job in enumerate(job_spec):
        print(f"Job {i+1}")
        test_ai_comment_extraction(job)