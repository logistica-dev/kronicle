def pytest_addoption(parser):
    parser.addoption(
        "--run-all",
        action="store_true",
        default=False,
        help="Run unit + integration tests together (coverage mode)",
    )
