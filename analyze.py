import json
from datetime import datetime
from datetime import timedelta

## how do we define if a repo is healthy or not?

## we can define a repo as healthy if it has:
# - more than 1000 stars
# - more than 100 contributors
# - more than 100 issues
# - more than 100 commits
# - more than 100 contributors
# - more than 100 commits
# - how often does the commit happens and stuff
# - if commit has happend in last 7 days and we comapre it against number of stars and issues we'll get if a repo is in good health or not

def analyze_repo_details(repo_details: any):
    for repo in repo_details:
        stars = repo['likes']
        contributors_count = len(repo['contributors'])
        issues_count = len(repo['issues'])
        commits_count = len(repo['commits'])
        last_commit_date = repo['last-commit-date']
        number_of_commits = repo['number-of-commits']
        print(number_of_commits)
        print(type(number_of_commits))
       

        print(f"\n{repo['name']}:")
        print(f"  Stars: {stars}")
        print(f"  Contributors: {contributors_count}")
        print(f"  Issues: {issues_count}")
        print(f"  Commits: {commits_count}")

        if stars > 1000 and contributors_count >= 30 and issues_count >= 20 and commits_count >= 20:
            if datetime.strptime(last_commit_date, '%Y-%m-%dT%H:%M:%SZ') > datetime.now() - timedelta(days=7):
                if number_of_commits >= 100:
                    healthy_repo_writer(repo)
            else:
                print(f"Repo {repo['name']} is not healthy")
        else:
            print(f"Repo {repo['name']} is not healthy")
                
 

def healthy_repo_writer(repo: any):
    with open('healthy_repos.json', 'a') as f:
        json.dump(repo, f, ensure_ascii=False, indent=4)
        f.write('\n')


if __name__ == "__main__":
    with open('repo_details.json', 'r', encoding='utf-8') as f:
        repo_details = json.load(f)
    print(f"Total repos loaded: {len(repo_details)}")
    print("Starting to analyze repo details...")
    analyze_repo_details(repo_details)
    print("All repo details analyzed successfully")