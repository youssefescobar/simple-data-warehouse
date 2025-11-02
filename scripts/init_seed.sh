#!/bin/bash

# ==============================================================================
# E-Commerce DB Initializer & Seeder
# ==============================================================================
# This script provides a menu to:
# 1. Clean the database volume directory.
# 2. Wait for the Postgres container to be ready.
# 3. Initialize a "simple" or "full" schema.
# 4. Seed the chosen schema with a corresponding Python script.
# ==============================================================================

# --- Configuration ---
# --- !! Please update these variables to match your environment !! ---
CONTAINER_NAME="production_db"
DB_USER="admin"
DB_NAME="xyz_store"
# This is the local directory on your machine that is bind-mounted
# into the Docker container as its data volume.
VOLUME_DIR="../prod_db"
# Path to the docker-compose directory
COMPOSE_DIR=".."
COMPOSE_FILE_PATH="$COMPOSE_DIR/docker-compose.yaml"
COMPOSE_FILE_PATH_YML="$COMPOSE_DIR/docker-compose.yml"
# ---------------------

# Exit immediately if a command exits with a non-zero status.
set -e
# Fail if any command in a pipeline fails (e.g., cat schema.sql | docker exec...)
set -o pipefail

# --- Colors for Logging ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# --- Logging Functions ---
log_info() {
    echo -e "${YELLOW}[INFO] $1${NC}"
}
log_success() {
    echo -e "${GREEN}[SUCCESS] $1${NC}"
}
log_error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

# --- Main Script Function ---
main() {
    # Step 1: Present the menu to the user
    log_info "Select the database version to initialize:"
    PS3="Enter your choice (1, 2, or 3): "
    options=("Simple Version" "Full Version" "Quit")
    
    select opt in "${options[@]}"; do
        case $opt in
            "Simple Version")
                SCHEMA_FILE="schema_simple.sql"
                SEED_FILE="seed_simple.py"
                VERSION_NAME="Simple"
                break
                ;;
            "Full Version")
                SCHEMA_FILE="schema_full.sql"
                SEED_FILE="seed_full.py"
                VERSION_NAME="Full"
                break
                ;;
            "Quit")
                log_info "Exiting script."
                exit 0
                ;;
            *) 
                echo -e "${RED}Invalid option $REPLY. Please try again.${NC}"
                ;;
        esac
    done

    log_info "You selected: $VERSION_NAME Version"

    # Step 2: Check that all required files and commands exist
    log_info "Checking for required files and commands..."
    if [ ! -f "$SCHEMA_FILE" ]; then
        log_error "Schema file not found: $SCHEMA_FILE"
    fi
    if [ ! -f "$SEED_FILE" ]; then
        log_error "Seed file not found: $SEED_FILE"
    fi
    if ! command -v docker &> /dev/null; then
        log_error "Docker command not found. Please install Docker and ensure it's in your PATH."
    fi
    if ! command -v docker-compose &> /dev/null; then
        log_error "docker-compose command not found. Please install it or use 'docker compose'."
    fi
    if ! command -v python &> /dev/null; then
        log_error "python command not found. Please install Python."
    fi
    log_success "All requirements are satisfied."

    # Step 3: Clean the volume directory as requested
    # This ensures Postgres starts in a clean state
    log_info "Cleaning database volume directory: $VOLUME_DIR"
    if [ -d "$VOLUME_DIR" ]; then
        log_info "Removing old volume directory..."
        rm -rf "$VOLUME_DIR"
    fi
    mkdir -p "$VOLUME_DIR"
    log_success "Clean volume directory created at $VOLUME_DIR"

    # Step 4: Start containers and wait for DB
    log_info "Checking for docker-compose file in $COMPOSE_DIR..."
    if [ ! -f "$COMPOSE_FILE_PATH" ] && [ ! -f "$COMPOSE_FILE_PATH_YML" ]; then
        log_error "docker-compose.yaml or docker-compose.yml not found in $COMPOSE_DIR. Cannot start containers."
    fi
    
    log_info "Starting containers via 'docker-compose up -d' from $COMPOSE_DIR..."
    # Run docker-compose from the directory where the file is located
    (cd "$COMPOSE_DIR" && docker-compose up -d)
    
    log_success "Containers started (or were already running)."
    log_info "----------------------------------------------------------------"
    log_info "The script will automatically wait until the database is ready."
    log_info "----------------------------------------------------------------"

    log_info "Waiting for PostgreSQL container '$CONTAINER_NAME' to be ready..."
    
    # This loop will run until the `pg_isready` command inside the container
    # returns a success code (0).
    until docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" -q 2>/dev/null; do
        log_info "Database is not ready yet. Waiting 5 seconds..."
        sleep 5
    done
    
    log_success "Database is ready!"

    sleep 3
    # Step 5: Apply the schema
    log_info "Applying schema '$SCHEMA_FILE' to database '$DB_NAME'..."
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" < "$SCHEMA_FILE"
    log_success "Schema applied successfully."

    sleep 3
    # Step 6: Run the seeder script
    log_info "Running seeder script '$SEED_FILE'..."
    python "$SEED_FILE"
    log_success "Seeding complete."

    echo -e "\n${GREEN}====================================================${NC}"
    log_success "All done! The '$VERSION_NAME' database is initialized and seeded."
    log_success "Container: $CONTAINER_NAME"
    log_success "Database: $DB_NAME"
    log_success "Schema: $SCHEMA_FILE"
    log_success "Seeder: $SEED_FILE"
    echo -e "${GREEN}====================================================${NC}"
}

# --- Run the main function ---
main



