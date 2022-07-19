import subprocess

def create_worker_credentials():
    """
    Create worker node credentials
    """
    # Create token for HTCondor auth
    run = subprocess.run(["sudo",
                          "condor_token_create",
                          "-identity",
                          "worker@cloud",
                          "-key",
                          "token_key"],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if run.returncode == 0:
        token = run.stdout.strip().decode('utf-8')
    else:
        raise Exception('condor_token_create failed with invalid return code')

    return token
