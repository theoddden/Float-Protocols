# Gateway Hardware v0.1.0 Changes

## Overview
The v0.1.0 schematic represents a major hardware revision with significant component changes from v0.0.4.

## Component Changes

### 1. Modem: BG95-S5 → GM02SP
- **Previous**: Quectel BG95-S5 (LTE-M/NTN modem)
- **New**: Sequans GM02SP (LTE-M/NTN modem)
- **Impact**: Complete driver rewrite required
- **Key Differences**:
  - Different AT command set
  - Different GPIO control pins
  - Different UART interface requirements
  - Different power sequencing

### 2. LoRa Transceiver: SX1262 → LR1121
- **Previous**: Semtech SX1262 (LoRa transceiver)
- **New**: Semtech LR1121 (LoRa transceiver with 2.4GHz support)
- **Impact**: Complete driver rewrite required
- **Key Differences**:
  - Different SPI register map
  - Different GPIO control pins
  - Additional 2.4GHz band support
  - Different packet handler

### 3. Unchanged Components
- **ATECC608B**: Secure element (I2C bus 1) - unchanged
- **RGB LED**: GPIO17 (Red), GPIO27 (Green), GPIO6 (Blue) - unchanged
- **Config Button**: GPIO18 - unchanged
- **Watchdog**: GPIO26 - unchanged
- **RS-485**: UART `/dev/ttyAMA1`, DE/RE GPIO5 - unchanged
- **RS-232**: UART `/dev/ttyAMA1` - unchanged

## GM02SP Pin Assignments (from schematic)
- **UART TX/RX**: Connected to CM5 GPIO14/GPIO15 (UART0)
- **SIM Interface**: GM02_SIM_CLK, GM02_SIM_RST, GM02_SIM_DATA
- **RESET**: GM02_RESET_N
- **STATUS**: GM02_STATUS
- **Power**: GND1, GND2

## LR1121 Pin Assignments (from schematic)
- **SPI Interface**: Connected to CM5 SPI0
  - MOSI: GPIO10
  - MISO: GPIO9
  - SCK: GPIO11
  - CS: GPIO8
- **Control Pins**:
  - DIO1: GPIO22
  - NRST: GPIO23
- **Antenna**: ANT_LORA

## Software Changes Required

### 1. Create New Modules
- `src/hardware/gm02sp.rs` - GM02SP modem driver
- `src/hardware/lr1121.rs` - LR1121 LoRa driver

### 2. Update Existing Modules
- `src/hardware/mod.rs` - Add new module declarations
- `src/hardware/bg95.rs` - Deprecate or remove
- `src/hardware/sx1262.rs` - Deprecate or remove
- `src/main.rs` - Update hardware supervisor initialization

### 3. Update Dependencies
- Add GM02SP-specific driver crates (if available)
- Add LR1121-specific driver crates (if available)
- Remove SX1262 and BG95-S5 driver dependencies

## Migration Strategy
1. Create new driver modules alongside existing ones
2. Implement feature flags to switch between old and new hardware
3. Test new drivers on v0.1.0 hardware
4. Remove old drivers once verified
