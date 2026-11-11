#!/usr/bin/env python3
"""Test an isolated Compose deployment and remove only its disposable volume."""
import datetime
import json
from pathlib import Path
import subprocess
import time
import uuid


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    project = 'soup-verify-' + uuid.uuid4().hex[:12]
    out = Path('results/checks') / (project + '-' + stamp)
    out.mkdir(parents=True)
    base = ['docker', 'compose', '-p', project, '-f', 'compose.yaml', '-f', 'compose.verify.yaml']
    records = []
    def run(args, check=True, timeout=180):
        command = base + args
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            output = exc.stdout or b''
            if isinstance(output, bytes): output = output.decode(errors='replace')
            result = subprocess.CompletedProcess(command, 124, output + '\nCommand timed out.\n')
        name = f'{len(records):03d}.log'
        (out / name).write_text(result.stdout)
        records.append({'command':command,'exit_code':result.returncode,'output':name})
        (out / 'commands.json').write_text(json.dumps(records,indent=2)+'\n')
        if check and result.returncode:
            raise RuntimeError(f'{command}: exit {result.returncode}; see {out / name}')
        return result
    def sql(query):
        return run(['exec','-T','db','psql','-U','soup','-d','soup','-Atc',query]).stdout.strip()
    def ready():
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            result = run(['exec','-T','api','wget','-q','-O','-','http://127.0.0.1:8080/readyz'],check=False)
            if result.returncode == 0 and json.loads(result.stdout)['status'] == 'ready':
                return
            time.sleep(2)
        raise RuntimeError('API readiness timed out')
    failure = None
    started = False
    try:
        run(['config','--quiet'])
        started = True
        run(['up','--build','-d'],timeout=900)
        ready()
        run(['--profile','verification','run','--rm','integration'],timeout=900)
        job = sql("INSERT INTO jobs(idempotency_key,request) VALUES('deployment-persistence',jsonb_build_object('duration_ms',100,'simulate_failure',false)) RETURNING id").splitlines()[0]
        deadline = time.monotonic()+60
        while sql(f"SELECT status FROM jobs WHERE id='{job}'") != 'succeeded':
            if time.monotonic() >= deadline: raise RuntimeError('deployment workers did not finish the persistence job')
            time.sleep(.5)
        expected = json.loads(run(['exec','-T','api','wget','-q','-O','-',f'http://127.0.0.1:8080/jobs/{job}']).stdout)
        run(['down'])  # Retain the volume for the persistence check.
        run(['up','-d'],timeout=180)
        ready()
        actual = json.loads(run(['exec','-T','api','wget','-q','-O','-',f'http://127.0.0.1:8080/jobs/{job}']).stdout)
        if actual != expected or sql(f"SELECT count(*) FROM job_effects WHERE job_id='{job}'") != '1':
            raise RuntimeError('persisted job/effect changed across deployment restart')
        run(['ps','--all'])
    except Exception as exc:
        failure = str(exc)
    finally:
        if started:
            try:
                run(['logs','--no-color'],check=False)
                run(['down','-v','--remove-orphans'])
            except Exception as exc:
                failure = (failure + '; ' if failure else '') + 'cleanup: ' + str(exc)
        (out / 'summary.json').write_text(json.dumps({'project':project,'success':failure is None,'error':failure},indent=2)+'\n')
    print(f'{out} success={failure is None} error={failure or ""}')
    return 1 if failure else 0


if __name__ == '__main__':
    raise SystemExit(main())
