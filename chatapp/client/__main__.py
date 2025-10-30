"""
Entry point for the chat client.
Can launch either CLI or GUI version.
"""
import sys
import argparse


def main():
    """Launch the chat client application.
    
    Parses command line arguments to determine whether to start
    in GUI or CLI mode. Defaults to GUI if no mode specified.
    
    Command line options:
        --mode {gui,cli}: Interface mode to use
        
    Side Effects:
        - Starts either GUI or CLI interface
        - Exits with error if invalid arguments
    """
    parser = argparse.ArgumentParser(description="gRPC Chat Client")
    parser.add_argument("--mode", choices=["gui", "cli"], default="gui",
                        help="Interface mode (default: gui)")
    args = parser.parse_args()
    
    if args.mode == "gui":
        from .gui import main as gui_main
        gui_main()
    else:
        from .cli import app
        app()


if __name__ == "__main__":
    main()
