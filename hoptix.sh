#!/bin/env bash

set -emu

RED='\033[31m'
BLUE='\033[34m'
GREEN='\033[32m'
YELLOW='\033[33m'
BOLD='\033[1m'
NC='\033[0m'

ROOT_DIR=/usr/bin/
DIR_RELEASE=$(pwd)/target/release
ROOT_BINARY=/usr/bin/hoptixd
CLI_BINARY=$DIR_RELEASE/hoptix
APP_NAME="hoptixd"
BINARY_PATH=$DIR_RELEASE/$APP_NAME
SERVICE_DIR=/etc/systemd/system
GROUP="hoptix"
SERVICE_PATH=$SERVICE_DIR/hoptixd.service

REAL_USER=${SUDO_USER:-$USER}
REAL_HOME=$(eval echo "~$REAL_USER")

HOPTIX_SERVICE_CONF=$(
  cat <<EOF
[Unit]
Description=Hoptix Download Manager Daemon
After=network.target

[Service]
Type=simple
ExecStart=$ROOT_BINARY
Restart=on-failure
RestartSec=5

User=$REAL_USER
Group=$GROUP
WorkingDirectory=$REAL_HOME

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
)

error_exit() {
  eprint "$1" >&2
  exit 1
}

printed() {
  echo -e "${GREEN}=> ${NC}${BOLD}$1${NC}"
}
yprint() {
  echo -e "[${GREEN}OK${NC}]:${BOLD}${BLUE}$1${NC}"
}
eprint() {
  echo -e "[${RED}ERROR${NC}]:${BOLD}${YELLOW}$1${NC}"
}

groupConf() {
  if ! getent group $GROUP &>/dev/null; then
    printed "group:$GROUP does not exist, creating it..."
    sudo groupadd $GROUP || error_exit "Failed to create group $GROUP"
  fi

  if ! groups $REAL_USER | grep -q "\b$GROUP\b"; then
    printed "user:$REAL_USER is not in group $GROUP, adding user to group..."
    sudo usermod -a -G $GROUP $REAL_USER || error_exit "Failed to add user $REAL_USER to group $GROUP"
  fi
}

config_cli() {
  if [[ -f $CLI_BINARY ]]; then
    printed "moving $CLI_BINARY to $ROOT_DIR"
    sudo mv $CLI_BINARY $ROOT_DIR || error_exit "can't move $CLI_BINARY to $ROOT_DIR"
  fi
}

config_hoptix_service() {
  printed "configuring hoptixd service..."
  echo "$HOPTIX_SERVICE_CONF" | sudo tee "$SERVICE_PATH" >/dev/null || error_exit "Failed to write $SERVICE_PATH"
  yprint "service configuration completed"
}

reload_daemon() {
  printed "reloading systemd daemons..."
  sudo systemctl daemon-reload
  sudo systemctl enable hoptixd.service
  sudo systemctl restart hoptixd.service
  yprint "hoptixd service started and enabled"
}

check_conf() {
  if [[ -f $SERVICE_PATH ]]; then
    data=$(cat $SERVICE_PATH)
    if [[ "$data" == "$HOPTIX_SERVICE_CONF" ]]; then
      yprint "$(basename $SERVICE_PATH) is already configured correctly"
      return 0
    fi
  fi
  return 1
}

if ! which cargo >/dev/null; then
  sys=$(uname -n)
  printed "cargo and rustup not found, installing..."
  if [[ $sys == "arch" ]]; then
    sudo pacman -S cargo rustup
  else
    error_exit "Unsupported system. Please install Rust and Cargo manually."
  fi
fi

printed "building project in release mode..."
if cargo build --release; then
  yprint "build successful"

  if [[ -f $BINARY_PATH ]]; then
    printed "moving $BINARY_PATH to $ROOT_BINARY"
    sudo mv $BINARY_PATH $ROOT_BINARY || error_exit "Failed to move $BINARY_PATH to $ROOT_BINARY"
  else
    error_exit "Binary $BINARY_PATH not found after build"
  fi

  config_cli

  if ! [[ -d $SERVICE_DIR ]]; then
    printed "creating $SERVICE_DIR"
    sudo mkdir -p $SERVICE_DIR
  fi

  groupConf

  if ! check_conf; then
    config_hoptix_service
  fi

  reload_daemon

  yprint "<--- Installation completed successfully --->"
  printed "You can now use: hoptix start <URL>"
  printed "Check service status: systemctl status hoptixd"
else
  eprint "Build failed! Please check Cargo output and fix errors."
  exit 1
fi
