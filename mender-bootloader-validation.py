#!/usr/bin/env python3

import datetime
import json
import logging
import os
import re
import subprocess
import sys
import tempfile

###############################################################################
# section start: script startup                                               #
#   note: this creates the logger which can be captured in closures!          #
###############################################################################
root_path = "/"
persistent_directory = "data"

logger = logging.getLogger(__name__)
logging.basicConfig(filename=os.path.join(root_path, persistent_directory, "validation.log"), encoding='utf-8', level=logging.DEBUG)
logger.info(f"Mender validation script started {datetime.datetime.now()}")
###############################################################################
# section end: script startup                                                 #
###############################################################################

###############################################################################
# section start: process running utilites                                     #
###############################################################################
def check_for_command(cmd):
    try:
        result = subprocess.run(['which', cmd], stdout=subprocess.PIPE)
        if result.returncode == 0:
            return True
        return False
    except: # technically this could hide some valid exceptions to handle, but lets move forward for now.
        return False

def run_command(cmd):
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        if result.returncode == 0:
            return True
        return False
    except: # technically this could hide some valid exceptions to handle, but lets move forward for now.
        return False

def run_command_get_output(cmd):
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        if result.returncode == 0:
            return result.stdout.decode().strip()
        return None
    except: # technically this could hide some valid exceptions to handle, but lets move forward for now.
        return None
    
def reboot():
    run_command(["reboot"])
###############################################################################
# section end: process running utilites                                       #
###############################################################################

###############################################################################
# section start: persistent config and state class                            #
###############################################################################
MENDER_CLIENT_CONFIG_FILE="etc/mender/mender.conf"
MENDER_DEVICE_CONFIG_FILE="var/lib/mender/mender.conf"
PERSISTENT_STATE_FILENAME="mender_validation_state.json"

# config keys:
ROOTFS_A_KEY = "RootfsPartA"
ROOTFS_B_KEY = "RootfsPartB"
# state keys:
PART_NUMBER_A_KEY = "PartitionNumberA"
PART_NUMBER_B_KEY = "PartitionNumberB"
SET_CMD_KEY = "SetCmd"
PRINT_CMD_KEY = "PrintCmd"
EXPECTED_ROOT_KEY = "ExpectedRoot"
VALIDATION_STEP_KEY="step"

class PersistentState:
    # the various steps
    STEP_NONE="none"
    STEP_INIT="init"
    STEP_TEST_SWITCH="test_switch"
    STEP_TEST_UPDATE="test_update"
    STEP_TEST_ROLLBACK="test_rollback"
    STEP_END="end"

    def __init__(self, logger: logging.Logger, root_directory="/", persistent_directory="data"):
        self.root_directory = root_directory
        self.logger = logger
        self.filename = os.path.join(root_directory, persistent_directory, PERSISTENT_STATE_FILENAME)
        # get device configuration
        self.config = {}
        logger.info("loading device configuration...")
        def read_file_int(relative_path):
            p = os.path.join(self.root_directory, relative_path)
            logger.info("reading file " + p)
            try:
                with open(p, 'r') as f:
                    self.config.update(json.load(f))
            except FileNotFoundError:
                pass
            except json.JSONDecodeError:
                pass

        read_file_int(MENDER_CLIENT_CONFIG_FILE)
        read_file_int(MENDER_DEVICE_CONFIG_FILE)
        # load state
        self.logger.info("loading persistent state...")
        self.state = self._load_state()
        # some sanity checking
        last_a = self._get_state(ROOTFS_A_KEY)
        last_b = self._get_state(ROOTFS_B_KEY)
        config_a = self.config.get(ROOTFS_A_KEY)
        config_b = self.config.get(ROOTFS_B_KEY)
        try:
            if config_a is None:
                self.logger.info(f"root filesystem A not found in  config, aborting")
                raise RuntimeError()
            if config_b is None:
                self.logger.info(f"root filesystem B not found in  config, aborting")
                raise RuntimeError()
            if last_a is not None and last_a != config_a:
                self.logger.info(f"root filesystem A in config {config_a} does not match last used one {last_a}, aborting")
                raise RuntimeError()
            if last_b is not None and last_b != config_b:
                self.logger.info(f"root filesystem B in config {config_b} does not match last used one {last_b}, aborting")
                raise RuntimeError()
        except RuntimeError:
            print(f"triggering config:\n{json.dumps(self.config, indent=2)}, triggering state:\n{json.dumps(self.state, indent=2)}")
            raise RuntimeError()
        self.logger.info(f"...state and config ready.")
    
    def create_initial_state(self):
        SET_CMD_UBOOT = "fw_setenv"
        PRINT_CMD_UBOOT = "fw_printenv"
        SET_CMD_GRUB = "grub-mender-grubenv-set"
        PRINT_CMD_GRUB = "grub-mender-grubenv-print"

        def extract_part_number(part):
            m = re.search(r'\d+$', part)
            # if the string ends in digits m will be a Match object, or None otherwise.
            if m is not None:
                return m.group()
            return None

        self.logger.info("using configuration:")
        self.logger.info(f"resulting in {json.dumps(self.config, indent=2)}")
        self.logger.info("initializing persistent state...")
        rfs_a = self.config.get(ROOTFS_A_KEY)
        rfs_b = self.config.get(ROOTFS_B_KEY)
        set_cmd = SET_CMD_GRUB
        print_cmd = PRINT_CMD_GRUB
        if check_for_command(PRINT_CMD_UBOOT):
            set_cmd = SET_CMD_UBOOT
            print_cmd = PRINT_CMD_UBOOT

        # extract number at the end of partitions - https://stackoverflow.com/a/14471236
        part_num_a = extract_part_number(rfs_a)
        part_num_b = extract_part_number(rfs_b)
        # save initial configuration
        self._set_state(SET_CMD_KEY, set_cmd)
        self._set_state(PRINT_CMD_KEY, print_cmd)
        self._set_state(ROOTFS_A_KEY, rfs_a)
        self._set_state(ROOTFS_B_KEY, rfs_b)
        self._set_state(PART_NUMBER_A_KEY, part_num_a)
        self._set_state(PART_NUMBER_B_KEY, part_num_b)
        self._set_step(self.STEP_NONE)
        self.logger.info(f"resulting in {json.dumps(self.state, indent=2)}")

    def _load_state(self):
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as f:
                return json.load(f)
        else:
            return {}

    def _save_state(self):
        with open(self.filename, 'w') as f:
            json.dump(self.state, f)

    def _set_state(self, key, value):
        self.state[key] = value
        self._save_state()

    def _get_state(self, key):
        return self.state.get(key)
    
    def get_root_part_device_a(self):
        return self._get_state(ROOTFS_A_KEY)
    
    def get_root_part_device_b(self):
        return self._get_state(ROOTFS_B_KEY)

    def get_root_part_number_a(self):
        return self._get_state(PART_NUMBER_A_KEY)

    def get_root_part_number_b(self):
        return self._get_state(PART_NUMBER_B_KEY)
    
    def get_env_set_cmd(self):
        return self._get_state(SET_CMD_KEY)

    def get_env_print_cmd(self):
        return self._get_state(PRINT_CMD_KEY)
    
    def get_step(self):
        return self._get_state(VALIDATION_STEP_KEY)
    
    def get_expected_root(self):
        return self._get_state(EXPECTED_ROOT_KEY)
    
    def set_expected_root(self, value):
        return self._set_state(EXPECTED_ROOT_KEY, value)
    
    def _set_step(self, step):
        self._set_state(VALIDATION_STEP_KEY, step)
    
    def next_step(self):
        s = self.get_step()
        self.logger.info(f"proceeding to next step from {s}")
        if s == self.STEP_NONE:
            self._set_step(self.STEP_INIT)
        elif s == self.STEP_INIT:
            self._set_step(self.STEP_TEST_SWITCH)
        elif s == self.STEP_TEST_SWITCH:
            self._set_step(self.STEP_TEST_UPDATE)
        elif s == self.STEP_TEST_UPDATE:
            self._set_step(self.STEP_TEST_ROLLBACK)
        elif s == self.STEP_TEST_ROLLBACK:
            self._set_step(self.STEP_END)
        elif s == self.STEP_END:
            pass
        else:
            self.logger.error(f"could not get next step after {s}, aborting")
            raise Exception
        s = self.get_step()
        self.logger.info(f"new step is {s}")
        return s
    
    def clean(self):
        os.remove(self.filename)
###############################################################################
# section end: persistent config and state class                              #
###############################################################################

###############################################################################
# section start: root partition helpers                                       #
###############################################################################
CURRENT_ROOT_A = "root_a"
CURRENT_ROOT_B = "root_b"
CURRENT_ROOT_UNDEFINED = "root_undefined"
current_root = CURRENT_ROOT_UNDEFINED

def identify_mounted_root(state: PersistentState):
    root = run_command_get_output(["stat", "-c", "%D", root_path])
    a = run_command_get_output(["stat", "-c", "%t%02T", state.get_root_part_device_a()])
    b = run_command_get_output(["stat", "-c", "%t%02T", state.get_root_part_device_b()])
    result = CURRENT_ROOT_UNDEFINED
    if root == a:
        result = CURRENT_ROOT_A
    elif root == b:
        result = CURRENT_ROOT_B
    logger.info(f"mount identification -  '/': {root}, '{state.get_root_part_device_a()}': {a}, '{state.get_root_part_device_b()}': {b}, result is {result}")
    return result

def set_env_variable(state: PersistentState, variable_name: str, value: str):
    return run_command([state.get_env_set_cmd(), variable_name, value])

def assert_env_variable(state: PersistentState, variable_name: str, expect: str):
    value = run_command_get_output([state.get_env_print_cmd(), variable_name])
    logger.info(f"checking env {variable_name} for {expect}, cmd result {value}")
    if value is None:
        return False

    vdict = dict(re.findall(r"^\s*(.*?)\s*=\s*(.*?)\s*$", value))
    logger.info(f"evaluation output to {vdict}")
    if vdict[variable_name] is None:
        return False
    
    if vdict[variable_name] == expect:
        logger.info("success!")
        return True
    
    return False
###############################################################################
# section end: root partition helpers                                         #
###############################################################################

###############################################################################
# section start: bootloader helpers                                           #
###############################################################################
INACTIVE_PART_NUMBER = "number"
INACTIVE_PART_DEVICE = "device"
INACTIVE_PART_IDENT = "ident"

ENV_KEY_BOOT_PART = "mender_boot_part"
ENV_KEY_BOOT_PART_HEX = "mender_boot_part_hex"
ENV_KEY_BOOTCOUNT = "bootcount"
ENV_KEY_UPGRADE = "upgrade_available"
def get_inactive_bootpart_info(state: PersistentState, current):
    if current == CURRENT_ROOT_A:
        return {
            INACTIVE_PART_NUMBER: state.get_root_part_number_b(),
            INACTIVE_PART_DEVICE: state.get_root_part_device_b(),
            INACTIVE_PART_IDENT: CURRENT_ROOT_B
        }
    elif current == CURRENT_ROOT_B:
        return {
            INACTIVE_PART_NUMBER: state.get_root_part_number_a(),
            INACTIVE_PART_DEVICE: state.get_root_part_device_a(),
            INACTIVE_PART_IDENT: CURRENT_ROOT_A
        }
    return None

def set_mender_bootpart(state: PersistentState, num: int):
    if not set_env_variable(state, ENV_KEY_BOOT_PART, str(num)):
        logger.info("failed to set mender_boot_part")
        return False
    if not set_env_variable(state, ENV_KEY_BOOT_PART_HEX, str(num)):
        logger.info("failed to set mender_boot_part_hex")
        return False
    return True
###############################################################################
# section end: bootloader helpers                                             #
###############################################################################

###############################################################################
# section start: load state                                                   #
###############################################################################
try:
    state = PersistentState(logger, root_path, persistent_directory)
except RuntimeError:
    logger.info("something bad happened when loading the last state, exiting now.")
    sys.exit(1)
###############################################################################
# section end: load state                                                     #
###############################################################################

###############################################################################
# section start: first stage - evaluate outcome/result of last state          #
#   where applicable                                                          #
###############################################################################
keep_going = True
fail_reason = None
current_root = identify_mounted_root(state)

# if applicable handle outcome of last step
s = state.get_step()
logger.info(f"starting evaluation of step {s}")
if s == None:
    state.create_initial_state()
elif s == state.STEP_TEST_SWITCH:
    # check if expected root file system matches after switch
    expected = state.get_expected_root()
    if expected == current_root:
        logger.info("switch test successful")
    else:
        fail_reason = "switch test failed"
        logger.info(fail_reason)
        keep_going = False
elif s == state.STEP_TEST_UPDATE:
    logger.info("tested update")
    # check the successful switch during the update
    expected = state.get_expected_root()
    if expected != current_root:
        fail_reason = f"update test did not match expected root: {expected}"
        logger.info(fail_reason)
        keep_going = False
    # check if the boot environment matches expectitations
    if not assert_env_variable(state, ENV_KEY_BOOTCOUNT, str(1)):
        fail_reason = f"failed {ENV_KEY_BOOTCOUNT} assertion"
        logger.info(fail_reason)
        keep_going = False
    if not assert_env_variable(state, ENV_KEY_UPGRADE, str(1)):
        fail_reason = f"failed {ENV_KEY_UPGRADE} assertion"
        logger.info(fail_reason)
        keep_going = False
    # clean up boot environment
    if keep_going and not set_env_variable(state, ENV_KEY_BOOTCOUNT, str(0)):
        fail_reason = f"failed to set {ENV_KEY_BOOTCOUNT}"
        logger.info(fail_reason)
        keep_going = False
    if keep_going and not set_env_variable(state, ENV_KEY_UPGRADE, str(0)):
        fail_reason = f"failed to set {ENV_KEY_UPGRADE}"
        logger.info(fail_reason)
        keep_going = False
    if keep_going:
        logger.info("update test successful")
elif s == state.STEP_TEST_ROLLBACK:
    # check the successful rollback!
    # 1: upgrade should not be marked as available anymore
    if not assert_env_variable(state, ENV_KEY_UPGRADE, str(0)):
        fail_reason = f"failed {ENV_KEY_UPGRADE} assertion"
        logger.info(fail_reason)
        keep_going = False
    # 2: check for the expected root filesystem
    expected = state.get_expected_root()
    if expected != current_root:
        fail_reason = f"rollback test did not match expected root: {expected}"
        logger.info(fail_reason)
        keep_going = False
    # 3: clean up bootloader environment
    if keep_going and not set_env_variable(state, ENV_KEY_BOOTCOUNT, str(0)):
        fail_reason = f"failed to set {ENV_KEY_BOOTCOUNT}"
        logger.info(fail_reason)
        keep_going = False
    if keep_going and not set_env_variable(state, ENV_KEY_UPGRADE, str(0)):
        fail_reason = f"failed to set {ENV_KEY_UPGRADE}"
        logger.info(fail_reason)
        keep_going = False
    if keep_going:
        logger.info("rollback test successful")
logger.info(f"ending evaluation of step {s}")
###############################################################################
# section end: first stage - evaluate outcome/result of last state            #
###############################################################################

###############################################################################
# section start: second stage - prepare device for next test step             #
###############################################################################
if keep_going:
    # go to next step
    s = state.next_step()
# prepare device for next test step
logger.info(f"starting prepartion of step {s}")
if s == state.STEP_INIT:
    logger.info("gathering some system information")
    logger.info(f"uname -a: {run_command_get_output(["uname", "-a"])}")
    with open('/etc/os-release', 'r') as file:
        logger.info(f"/etc/os-release:\n{file.read()}")
elif s == state.STEP_TEST_SWITCH:
    inactive = get_inactive_bootpart_info(state, current_root)
    if inactive is not None:
        if set_mender_bootpart(state, inactive[INACTIVE_PART_NUMBER]):
            state.set_expected_root(inactive[INACTIVE_PART_IDENT])
        else:
            fail_reason = f"failed to set boot partition {inactive[INACTIVE_PART_IDENT]}"
            logger.info(fail_reason)
            keep_going = False
    else:
        fail_reason = "could not identify partition numbers for switch, aborting"
        logger.info(fail_reason)
        keep_going = False
elif s == state.STEP_TEST_UPDATE:
    if keep_going and not set_env_variable(state, ENV_KEY_BOOTCOUNT, str(0)):
        fail_reason = "failed to set {ENV_KEY_BOOTCOUNT}"
        logger.info(fail_reason)
        keep_going = False

    if keep_going and not set_env_variable(state, ENV_KEY_UPGRADE, str(1)):
        fail_reason = "failed to set {ENV_KEY_UPGRADE}"
        logger.info(fail_reason)
        keep_going = False

    inactive = get_inactive_bootpart_info(state, current_root)
    if keep_going and inactive is not None:
        if set_mender_bootpart(state, inactive[INACTIVE_PART_NUMBER]):
            state.set_expected_root(inactive[INACTIVE_PART_IDENT])
        else:
            fail_reason = f"failed to set boot partition {inactive[INACTIVE_PART_IDENT]}"
            logger.info(fail_reason)
            keep_going = False
    else:
        fail_reason = "could not identify partition numbers for update, aborting"
        logger.info(fail_reason)
        keep_going = False
elif s == state.STEP_TEST_ROLLBACK:
    # preparing for rollback test
    BOOT_DIRECTORY = "boot"
    BOOT_DIRECTORY_DEFUNCT = "boot-defunct"
    inactive = get_inactive_bootpart_info(state, current_root)
    if keep_going and inactive is not None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            # 1: create temporary directory for mounting the inactive partition
            logger.info('created temporary directory', tmpdirname)
            # 2: mount inactive patition
            if not run_command(["mount", inactive[INACTIVE_PART_DEVICE], tmpdirname]):
                fail_reason = f"failed to mount {inactive[INACTIVE_PART_DEVICE]} to {tmpdirname}, aborting"
                logger.info(fail_reason)
                keep_going = False
            # 3: break the inactive partition by renaming the boot directory -> bootloader won't be able to load a kernel anymore
            if keep_going:
                try:
                    os.rename(os.path.join(tmpdirname, BOOT_DIRECTORY), os.path.join(tmpdirname, BOOT_DIRECTORY_DEFUNCT))
                except: # again, could be in finer granularity
                    fail_reason = f"failed to rename {BOOT_DIRECTORY} in {tmpdirname}, aborting"
                    logger.info(fail_reason)
                    keep_going = False
            # 4: unmount inactive partition
            if keep_going and not run_command(["umount", tmpdirname]):
                fail_reason = f"failed to unmount {tmpdirname}"
                logger.info(fail_reason)
                keep_going = False
            # 5: set update available in bootloader
            if keep_going and not set_env_variable(state, ENV_KEY_BOOTCOUNT, str(0)):
                fail_reason = f"failed to set {ENV_KEY_BOOTCOUNT}"
                logger.info(fail_reason)
                keep_going = False
            if keep_going and not set_env_variable(state, ENV_KEY_UPGRADE, str(1)):
                fail_reason = f"failed to set {ENV_KEY_UPGRADE}"
                logger.info(fail_reason)
                keep_going = False
            # 6: instruct bootloader to switch to the inactive partition
            if keep_going and not set_mender_bootpart(state, inactive[INACTIVE_PART_NUMBER]):
                fail_reason = f"failed to set boot partition {inactive[INACTIVE_PART_IDENT]}"
                logger.info(fail_reason)
                keep_going = False
            # 7: set expected root the the current, active one (as we expect the bootloader to roll back)
            if keep_going:
                state.set_expected_root(current_root)
elif s == state.STEP_END:
    logger.info("ending")
    keep_going = False
logger.info(f"ending prepartion of step {s}")
###############################################################################
# section end: second stage - prepare device for next test step               #
###############################################################################

if keep_going:
    reboot()
else:
    # we are done, no need to invoke the script again.
    run_command(["systemctl", "disable", "mender-bootloader-validation.service"])
    state.clean()
    if fail_reason == None:
        logger.info("BOOTLOADER VALIDATION: SUCCESS")
    else:
        logger.info(f"BOOTLOADER VALIDATION: FAILURE - {fail_reason}")
