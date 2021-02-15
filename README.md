# mongo_flask
Data pipeline using MongoDB and Flask

How to dockerize:

1. In windows powershell by admin rights, install chocolatey https://chocolatey.org/install
1. install pack https://buildpacks.io/docs/tools/pack/ (Add . $(pack completion)) to your .bashrc or .bash_profile)
1. follow these steps: https://buildpacks.io/docs/app-developer-guide/build-an-app/ \
  pack build part1_tomongodb.py --path src/ --builder cnbs/sample-builder:bionic

