from hourglass import create_app


if __name__ == "__main__":
    hourglass = create_app()
    hourglass.flask.run(debug=True)
