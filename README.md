# ReNomPI

ReNom PI is ReNom interface for PISystem

## Install

## Setup PISystem Environment
Setup PIsystem environment following bellow url.

https://pisquare.osisoft.com/community/all-things-pi/japanese/japanpidevelopersclubkaihatsu

Also, you need following settings.

- Create a new attributtion for prediction data which can be inserted future data and allow update.
- Set PI Web API Server to accept from development environment which usually deny for CSRF.
- Set PI Web API Server to accept HTTP POST request. 
- Set Basic authentication for PI Web API Server.

## Install from source
For installing ReNomPI, download the repository from following url.

`git clone https://gitlab.com/grid-devs/ReNomPI.git`

Move into ReNomPI directory.
`cd ReNomPI`

Then install all required packages.

`python setup.py install`
