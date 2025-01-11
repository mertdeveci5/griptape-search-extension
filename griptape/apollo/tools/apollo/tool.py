from __future__ import annotations
from griptape.artifacts import ListArtifact, ErrorArtifact, TextArtifact
from griptape.tools import BaseTool
from griptape.utils.decorators import activity
from schema import Schema, Literal, Optional
from attr import define, field
import requests
import logging


@define
class ApolloClient(BaseTool):
    SEARCH_ENDPOINT = "https://api.apollo.io/v1/mixed_people/search"
    ORGANIZATION_SEARCH_ENDPOINT = "https://api.apollo.io/api/v1/mixed_companies/search"
    BULK_MATCH_ENDPOINT = "https://api.apollo.io/api/v1/people/bulk_match"

    api_key: str = field(kw_only=True)
    timeout: int = field(default=10, kw_only=True)

    @activity(
        config={
            "description": "Searches for people on Apollo.io based on specified criteria",
            "schema": Schema(
                {
                    Literal(
                        "person_titles",
                        description="Job titles held by the people you want to find (e.g., ['marketing manager', 'research analyst'])",
                    ): [str],
                    Literal(
                        "person_locations",
                        description="Locations where people live (e.g., ['chicago', 'london', 'United States', 'Turkey'])",
                    ): [str],
                    Literal(
                        "organization_locations",
                        description="Headquarters locations of companies (e.g., ['chicago', 'london', 'United States', 'Turkey'])",
                    ): [str],
                    Literal(
                        "organization_num_employees_ranges",
                        description="Company size ranges (e.g., ['1,10', '11,50', '51,200']). Always in the format 'min,max'. Fortune 500 companies or enterprises are '5000, 100000",
                    ): [str],
                    Literal(
                        "q_organization_keyword_tags",
                        description="Keywords that are associated with the company, never the name of the company (e.g., ['software', 'sales', 'artificial intelligence']). Never put company names or website URL here",
                    ): [str],
                    Literal(
                        "q_organization_domains",
                        description="List of company website URLs to search within (e.g., ['monad.xyz', 'arbitrum.io']). Will be joined with newlines in the request.",
                    ): [str],
                }
            ),
        }
    )
    def search_people(self, params: dict) -> ListArtifact | ErrorArtifact:
        payload = {}

        # Handle person titles
        if person_titles := params["values"].get("person_titles"):
            payload["person_titles"] = person_titles

        # Handle person locations
        if person_locations := params["values"].get("person_locations"):
            payload["person_locations"] = person_locations

        # Handle organization locations
        if org_locations := params["values"].get("organization_locations"):
            payload["organization_locations"] = org_locations

        # Handle organization employee ranges
        if emp_ranges := params["values"].get("organization_num_employees_ranges"):
            payload["organization_num_employees_ranges"] = emp_ranges

        # Handle keywords - send as array, not joined string
        if keywords := params["values"].get("q_organization_keyword_tags"):
            payload["q_organization_keyword_tags"] = keywords  # Remove the .join()

        # Handle organization domains - join with newlines
        if domains := params["values"].get("q_organization_domains"):
            payload["q_organization_domains"] = "\n".join(domains)

        # Set fixed pagination parameters
        payload["page"] = 1
        payload["per_page"] = 10
        payload["contact_email_status"] = ["verified"]

        headers = {
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
        }

        try:
            response = requests.post(
                self.SEARCH_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            logging.info(f"Request payload: {payload}")
            response.raise_for_status()
            data = response.json()
            logging.info(f"Response data: {data}")
            pagination = data.get("pagination", {})
            total_entries = pagination.get("total_entries", 0)
            total_pages = pagination.get("total_pages", 0)
            logging.info(f"Total entries: {total_entries}")
            # Assuming the response contains a list of people
            people_data = data.get("people", [])
            logging.info(f"People data length: {len(people_data)}")
            formatted_people = []

            # Add pagination information as the first item in the list
            pagination_info = TextArtifact(
                f"""
                Pagination Information:
                - Total Profiles Found: {total_entries}
                - Total Pages: {total_pages}
                - Current Page: {pagination.get('page', 1)}
                - Results Per Page: {pagination.get('per_page', 10)}
                """
            )
            formatted_people.append(pagination_info)

            for person in people_data:
                org = person.get("organization", {})
                formatted_person = {
                    "name": person.get("name"),
                    "title": person.get("title"),
                    "headline": person.get("headline"),
                    "email_status": person.get("email_status"),
                    "linkedin_url": person.get("linkedin_url"),
                    "location": f"{person.get('city', '')}, {person.get('state', '')}, {person.get('country', '')}",
                    "company": {
                        "name": org.get("name"),
                        "website": org.get("website_url"),
                        "linkedin": org.get("linkedin_url"),
                    },
                    "seniority": person.get("seniority"),
                    "departments": person.get("departments", []),
                    "functions": person.get("functions", []),
                }
                formatted_people.append(
                    TextArtifact(
                        f"""
                    Name: {formatted_person['name']}
                    Title: {formatted_person['title']}
                    Headline: {formatted_person['headline']}
                    Email Status: {formatted_person['email_status']}
                    LinkedIn: {formatted_person['linkedin_url']}
                    Location: {formatted_person['location']}
                    Company: {formatted_person['company']['name']}
                    Company Website: {formatted_person['company']['website']}
                    Company LinkedIn: {formatted_person['company']['linkedin']}
                    Seniority: {formatted_person['seniority']}
                    Departments: {', '.join(formatted_person['departments'])}
                    Functions: {', '.join(formatted_person['functions'])}
                    """
                    )
                )

            return ListArtifact(formatted_people)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            logging.error(f"Response content: {response.text}")
            return ErrorArtifact(f"Request failed: {e}")
        except ValueError:
            logging.error("Failed to decode JSON from response")
            return ErrorArtifact("Failed to decode JSON from response")

    @activity(
        config={
            "description": "Searches for companies and organizations on Apollo.io based on specified criteria",
            "schema": Schema(
                {
                    Literal(
                        "organization_num_employees_ranges",
                        description="Company size ranges (e.g., ['1,10', '11,50', '51,200']). Always in the format 'min,max'.",
                    ): [str],
                    Literal(
                        "organization_locations",
                        description="Headquarters locations of companies (e.g., ['chicago', 'london', 'United States'])",
                    ): [str],
                    Literal(
                        "organization_not_locations",
                        description="Locations to exclude from search (e.g., ['ireland', 'minnesota'])",
                    ): [str],
                    Literal(
                        "q_organization_keyword_tags",
                        description="Keywords associated with companies (e.g., ['mining', 'sales strategy', 'consulting'])",
                    ): [str],
                }
            ),
        }
    )
    def search_organizations(self, params: dict) -> ListArtifact | ErrorArtifact:
        payload = {}

        # Handle organization employee ranges
        if emp_ranges := params["values"].get("organization_num_employees_ranges"):
            payload["organization_num_employees_ranges"] = emp_ranges

        # Handle organization locations
        if org_locations := params["values"].get("organization_locations"):
            payload["organization_locations"] = org_locations

        # Handle excluded locations
        if not_locations := params["values"].get("organization_not_locations"):
            payload["organization_not_locations"] = not_locations

        # Handle keywords
        if keywords := params["values"].get("q_organization_keyword_tags"):
            payload["q_organization_keyword_tags"] = keywords

        # Set fixed pagination parameters
        payload["page"] = 1
        payload["per_page"] = 10

        headers = {
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
        }

        try:
            response = requests.post(
                self.ORGANIZATION_SEARCH_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            logging.info(f"Request payload: {payload}")
            response.raise_for_status()
            data = response.json()
            logging.info(f"Response data: {data}")
            pagination = data.get("pagination", {})
            total_entries = pagination.get("total_entries", 0)
            total_pages = pagination.get("total_pages", 0)
            logging.info(f"Total entries: {total_entries}")

            organizations_data = data.get("organizations", [])
            logging.info(f"Organizations data length: {len(organizations_data)}")
            formatted_orgs = []

            # Add pagination information as the first item in the list
            pagination_info = TextArtifact(
                f"""
                Pagination Information:
                - Total Organizations Found: {pagination.get('total_entries', 0)}
                """
            )
            formatted_orgs.append(pagination_info)

            for org in organizations_data:
                # Get primary phone info
                phone_info = org.get("primary_phone", {})
                phone_number = (
                    phone_info.get("number") if phone_info else org.get("phone")
                )

                formatted_org = {
                    "name": org.get("name"),
                    "website": org.get("website_url"),
                    "linkedin_url": org.get("linkedin_url"),
                    "twitter_url": org.get("twitter_url"),
                    "facebook_url": org.get("facebook_url"),
                    "blog_url": org.get("blog_url"),
                    "phone": phone_number,
                    "languages": org.get("languages", []),
                    "alexa_ranking": org.get("alexa_ranking"),
                    "founded_year": org.get("founded_year"),
                    "publicly_traded_symbol": org.get("publicly_traded_symbol"),
                    "publicly_traded_exchange": org.get("publicly_traded_exchange"),
                    "logo_url": org.get("logo_url"),
                    "primary_domain": org.get("primary_domain"),
                }
                formatted_orgs.append(
                    TextArtifact(
                        f"""
                    Name: {formatted_org['name']}
                    Website: {formatted_org['website']}
                    LinkedIn: {formatted_org['linkedin_url']}
                    Twitter: {formatted_org['twitter_url']}
                    Facebook: {formatted_org['facebook_url']}
                    Blog: {formatted_org['blog_url']}
                    Phone: {formatted_org['phone']}
                    Languages: {', '.join(formatted_org['languages'])}
                    Alexa Ranking: {formatted_org['alexa_ranking']}
                    Founded Year: {formatted_org['founded_year']}
                    Stock Symbol: {formatted_org['publicly_traded_symbol']}
                    Stock Exchange: {formatted_org['publicly_traded_exchange']}
                    Logo URL: {formatted_org['logo_url']}
                    Primary Domain: {formatted_org['primary_domain']}
                    """
                    )
                )

            return ListArtifact(formatted_orgs)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            logging.error(f"Response content: {response.text}")
            return ErrorArtifact(f"Request failed: {e}")
        except ValueError:
            logging.error("Failed to decode JSON from response")
            return ErrorArtifact("Failed to decode JSON from response")

    @activity(
        config={
            "description": "Finds people's data with emails and additional information. Either LinkedIn URL or email is required.",
            "schema": Schema(
                {
                    Literal(
                        "details",
                        description="List of people to enrich. Must provide either LinkedIn URL or email for each person.",
                    ): [
                        Schema(
                            {
                                Optional("email"): str,
                                Optional("linkedin_url"): str,
                            },
                            ignore_extra_keys=True,
                        )
                    ],
                }
            ),
        }
    )
    def enrich_people(self, params: dict) -> ListArtifact | ErrorArtifact:
        details = params["values"]["details"]

        # Clean the details and validate required identifiers
        cleaned_details = []
        for person in details:
            cleaned_person = {
                k: v
                for k, v in person.items()
                if v and v.strip()  # Keep only non-empty strings
            }

            # Check if either linkedin_url or email is present
            if not (cleaned_person.get("linkedin_url") or cleaned_person.get("email")):
                return ErrorArtifact(
                    "Each person must have either a LinkedIn URL or email address"
                )

            cleaned_details.append(cleaned_person)

        payload = {
            "details": cleaned_details,
            "reveal_personal_emails": False,
            "reveal_phone_number": False,
        }

        headers = {
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
        }

        try:
            response = requests.post(
                self.BULK_MATCH_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            logging.info(f"Request payload: {payload}")
            response.raise_for_status()
            data = response.json()
            logging.info(f"Response data: {data}")

            results = []

            # Add summary information
            summary = TextArtifact(
                f"""
                Enrichment Summary:
                - Total Requested: {data.get('total_requested_enrichments', 0)}
                - Successfully Enriched: {data.get('unique_enriched_records', 0)}
                - Missing Records: {data.get('missing_records', 0)}
                - Credits Consumed: {data.get('credits_consumed', 0)}
                """
            )
            results.append(summary)

            # Process each matched person
            for person in data.get("matches", []):
                current_job = next(
                    (
                        job
                        for job in person.get("employment_history", [])
                        if job.get("current")
                    ),
                    {},
                )

                person_info = TextArtifact(
                    f"""
                    Name: {person.get('name')}
                    Email: {person.get('email')}
                    LinkedIn: {person.get('linkedin_url')}
                    Current Title: {person.get('title')}
                    Current Company: {current_job.get('organization_name')}
                    Location: {person.get('city', '')}, {person.get('state', '')}, {person.get('country', '')}
                    Departments: {', '.join(person.get('departments', []))}
                    Seniority: {person.get('seniority')}
                    Functions: {', '.join(person.get('functions', []))}
                    Is Likely to Engage: {person.get('is_likely_to_engage', False)}
                    """
                )
                results.append(person_info)

            return ListArtifact(results)

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            logging.error(f"Response content: {response.text}")
            return ErrorArtifact(f"Request failed: {e}")
        except ValueError:
            logging.error("Failed to decode JSON from response")
            return ErrorArtifact("Failed to decode JSON from response")
