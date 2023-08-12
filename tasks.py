import invoke


@invoke.task
def build(c):
    c.run('docker-compose build jobsearch')


@invoke.task
def tag(c):
    c.run('docker tag myapp registry.digitalocean.com/myapp')


@invoke.task
def push(c):
    c.run('docker push registry.digitalocean.com/myapp')


@invoke.task
def deploy(c):
    build(c)
    tag(c)
    push(c)
