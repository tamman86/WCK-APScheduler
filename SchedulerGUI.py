import FreeSimpleGUI as sg
import pandas as pd
import googlemaps
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, COIN_CMD
import sys
import os
from datetime import datetime, timedelta, time
import json
import time as tm

# --- CONFIGURATION ---
GEOCODE_CACHE_FILE = 'geocode_cache.json'
STABLE_COSTS_FILE = 'stable_travel_costs.csv'


# The hardcoded API_KEY variable has been removed from here.

# --- Geocoding Cache Helper Functions ---
def load_geocode_cache():
    try:
        with open(GEOCODE_CACHE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_geocode_cache(cache_data):
    with open(GEOCODE_CACHE_FILE, 'w') as f:
        json.dump(cache_data, f, indent=4)


# --- Main Geocoding Process ---
def run_geocoding_process(window, values):
    print("--- Starting Geocoding Pre-Process ---")
    excel_path = values['-FILE_PATH-']
    api_key = values['-API_KEY-']  # Get API key from GUI

    if not api_key:
        print("ERROR: Please enter your Google Maps API key.\n")
        return
    if not excel_path:
        print("ERROR: Please select a data file first.\n")
        return

    try:
        df = pd.read_excel(excel_path, sheet_name="Recipients")
        geocode_cache = load_geocode_cache()
        new_coords_added = False

        if 'Latitude' not in df.columns: df['Latitude'] = None
        if 'Longitude' not in df.columns: df['Longitude'] = None

        gmaps = googlemaps.Client(key=api_key)  # Use key from GUI

        for index, row in df.iterrows():
            if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
                address = row['Full Address']
                if not isinstance(address, str) or address.strip() == '': continue

                if address in geocode_cache:
                    print(f"CACHE HIT: Found coordinates for '{address}'")
                    lat, lng = geocode_cache[address]['lat'], geocode_cache[address]['lng']
                else:
                    print(f"API CALL: Geocoding new address '{address}'...")
                    geocode_result = gmaps.geocode(address)
                    if geocode_result:
                        location = geocode_result[0]['geometry']['location']
                        lat, lng = location['lat'], location['lng']
                        geocode_cache[address] = {'lat': lat, 'lng': lng}
                        new_coords_added = True
                    else:
                        print(f"WARNING: Could not geocode '{address}'")
                        lat, lng = None, None

                df.at[index, 'Latitude'] = lat
                df.at[index, 'Longitude'] = lng

        if new_coords_added:
            save_geocode_cache(geocode_cache)
            print("Saving updated coordinates back to the 'Recipients' sheet...")
            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name="Recipients", index=False)
        else:
            print("No new addresses found to geocode.")

        print("--- Geocoding Pre-Process Complete ---\n")
    except Exception as e:
        print(f"\n--- AN ERROR OCCURRED DURING GEOCODING ---")
        print(str(e))


# --- Main Optimization Process ---
def start_full_process(window, values):
    try:
        excel_path = values['-FILE_PATH-']
        use_stable_cache = values['-USE_CACHE-']
        api_key = values['-API_KEY-']

        if not api_key:
            print("ERROR: Please enter your Google Maps API key.\n")
            return

        try:
            days_ahead = int(values['-DAYS_AHEAD-'])
            if not 1 <= days_ahead <= 3: raise ValueError("Days Ahead must be between 1 and 3.")
        except ValueError as e:
            print(f"ERROR: Invalid 'Days Ahead' value. {e}\n")
            return

        if not excel_path:
            print("ERROR: Please select a data file first.\n")
            return

        print("--- Starting Optimization Process ---\n")

        today = datetime.now()
        future_date = today + timedelta(days=days_ahead)
        departure_datetime = datetime.combine(future_date.date(), time(11, 0))
        departure_timestamp = int(departure_datetime.timestamp())
        day_filter_str = future_date.strftime('%a').replace("Tue", "Tues")
        day_full_name = future_date.strftime('%A')
        formatted_date = future_date.strftime('%Y-%m-%d')
        sheet_title = f"Delivery Schedule for {day_full_name}, {formatted_date}"

        print(f"Forecasting for {days_ahead}-day(s) ahead: {day_full_name}")

        suppliers_df = pd.read_excel(excel_path, sheet_name="Suppliers")
        recipients_df = pd.read_excel(excel_path, sheet_name="Recipients")

        available_suppliers_df = suppliers_df[
            suppliers_df['Availability'].str.contains(day_filter_str, na=False)].copy()
        if available_suppliers_df.empty:
            print(f"ERROR: No suppliers found with availability for '{day_filter_str}'.\n")
            return

        print(f"Found {len(available_suppliers_df)} available suppliers and {len(recipients_df)} recipients.\n")

        if use_stable_cache:
            print("Step 2: Using stable travel times from cache file...")
            try:
                cost_df = pd.read_csv(STABLE_COSTS_FILE)
                print("Successfully loaded travel times from cache.\n")
            except FileNotFoundError:
                print(
                    f"ERROR: Cache file '{STABLE_COSTS_FILE}' not found. Please run once in normal mode to create it.\n")
                return
        else:
            print(
                f"Step 2: Requesting predictive travel data in batches for {departure_datetime.strftime('%Y-%m-%d %I:%M %p')}...")
            gmaps = googlemaps.Client(key=api_key)

            all_recipient_coords = list(zip(recipients_df['Latitude'], recipients_df['Longitude']))
            all_supplier_coords = list(zip(available_suppliers_df['Lat'], available_suppliers_df['Long']))

            all_results = []

            # --- MODIFIED: Reduced batch sizes ---
            batch_size = 10

            for i in range(0, len(available_suppliers_df), batch_size):
                supplier_batch_df = available_suppliers_df.iloc[i:i + batch_size]
                supplier_batch_coords = all_supplier_coords[i:i + batch_size]

                for j in range(0, len(recipients_df), batch_size):
                    recipient_batch_df = recipients_df.iloc[j:j + batch_size]
                    recipient_batch_coords = all_recipient_coords[j:j + batch_size]

                    print(
                        f"  - Processing supplier batch {i // batch_size + 1} vs recipient batch {j // batch_size + 1}...")

                    matrix_result = gmaps.distance_matrix(
                        origins=supplier_batch_coords,
                        destinations=recipient_batch_coords,
                        mode="driving",
                        departure_time=departure_timestamp
                    )

                    for supplier_idx, row in enumerate(matrix_result['rows']):
                        for recipient_idx, element in enumerate(row['elements']):
                            if element['status'] == 'OK':
                                all_results.append({
                                    'Supplier': supplier_batch_df.iloc[supplier_idx]['Name'],
                                    'Recipient': recipient_batch_df.iloc[recipient_idx]['Name'],
                                    'TravelTime_Minutes': round(element['duration']['value'] / 60, 2)
                                })

                    # --- MODIFIED: Add a 1-second delay to avoid rate-limiting ---
                    tm.sleep(1)

            print("\nProcessing travel data and intelligently updating stable cache file...\n")
            new_cost_df = pd.DataFrame(all_results)

            try:
                cached_cost_df = pd.read_csv(STABLE_COSTS_FILE)
                merged_df = pd.merge(new_cost_df, cached_cost_df, on=['Supplier', 'Recipient'], how='left',
                                     suffixes=('_new', '_cached'))

                def get_best_time(row):
                    if pd.notna(row['TravelTime_Minutes_cached']):
                        return min(row['TravelTime_Minutes_new'], row['TravelTime_Minutes_cached'])
                    else:
                        return row['TravelTime_Minutes_new']

                merged_df['TravelTime_Minutes'] = merged_df.apply(get_best_time, axis=1)
                cost_df = new_cost_df
                final_cache_df = merged_df[['Supplier', 'Recipient', 'TravelTime_Minutes']]
            except FileNotFoundError:
                print("No existing cache file found. Creating a new one.")
                cost_df = new_cost_df
                final_cache_df = new_cost_df

            final_cache_df.to_csv(STABLE_COSTS_FILE, index=False)
            print("Stable travel cost file has been updated with the best available times.")

        print("Step 3: Formulating and solving the optimization problem...")
        recipients_df['Standard Meal'] = recipients_df['Standard Meal'].fillna(0).astype(int)
        available_suppliers_df['Capacity'] = available_suppliers_df['Capacity'].fillna(0).astype(int)

        suppliers = available_suppliers_df['Name'].tolist()
        recipients = recipients_df['Name'].tolist()
        supply = pd.Series(available_suppliers_df.Capacity.values, index=available_suppliers_df.Name).to_dict()
        demand = pd.Series(recipients_df['Standard Meal'].values, index=recipients_df.Name).to_dict()
        costs = cost_df.set_index(['Supplier', 'Recipient'])['TravelTime_Minutes'].to_dict()
        costs = {s: {r: costs.get((s, r), 99999) for r in recipients} for s in suppliers}

        model = LpProblem(name="Meal-Delivery-Optimization", sense=LpMinimize)
        routes = LpVariable.dicts("Route", (suppliers, recipients), lowBound=0, cat='Integer')
        model += lpSum([routes[s][r] * costs[s][r] for s in suppliers for r in recipients]), "Total_Travel_Cost"
        for s in suppliers: model += lpSum([routes[s][r] for r in recipients]) <= supply[s], f"Supply_{s}"
        for r in recipients: model += lpSum([routes[s][r] for s in suppliers]) == demand[r], f"Demand_{r}"

        solver = None
        # Check if running as a PyInstaller bundle
        if getattr(sys, 'frozen', False):
            # If bundled, the path is to the temporary folder _MEIPASS
            solver_path = os.path.join(sys._MEIPASS, 'cbc.exe')
            solver = COIN_CMD(path=solver_path)
        else:
            # If running as a normal .py script, PuLP's default is fine on Windows
            # but we still check for the Mac case
            if sys.platform == "darwin":
                if os.path.exists('/opt/homebrew/bin/cbc'):
                    solver = COIN_CMD(path='/opt/homebrew/bin/cbc')
                elif os.path.exists('/usr/local/bin/cbc'):
                    solver = COIN_CMD(path='/usr/local/bin/cbc')

        model.solve(solver)
        print(f"Solver status: {LpStatus[model.status]}\n")

        if LpStatus[model.status] == 'Optimal':
            plan_sheet_name = f"DeliveryPlan_{days_ahead}-Day"
            print(f"Step 4: Optimal solution found! Saving to '{plan_sheet_name}' sheet...")

            delivery_plan = [{'Supplier': s, 'Deliver_To_Recipient': r, 'NumberOfMeals': int(routes[s][r].varValue)} for
                             s in suppliers for r in recipients if routes[s][r].varValue > 0]
            plan_df = pd.DataFrame(delivery_plan)

            title_df = pd.DataFrame([sheet_title] + [None] * (len(plan_df.columns) - 1)).T
            title_df.columns = plan_df.columns
            final_df = pd.concat([title_df, plan_df], ignore_index=True)

            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                final_df.to_excel(writer, sheet_name=plan_sheet_name, index=False, header=[None] * len(plan_df.columns))
                plan_df.to_excel(writer, sheet_name=plan_sheet_name, startrow=2, index=False)

            print("\n--- PROCESS COMPLETE ---")
            print(f"The schedule has been saved successfully to the '{plan_sheet_name}' sheet.")
        else:
            print("\n--- PROCESS FAILED ---")
            print("Could not find an optimal solution. Check if total supply can meet total demand.")

    except Exception as e:
        print(f"\n--- AN ERROR OCCURRED DURING OPTIMIZATION ---")
        print(str(e))


# --- MODIFIED: GUI Layout with API Key Field ---
sg.theme("SystemDefault")

api_key_frame = [
    sg.Text("Google Maps API Key:"),
    sg.Input(key='-API_KEY-', password_char='*', expand_x=True)
]

layout = [
    [sg.Frame("API Configuration", [api_key_frame], expand_x=True)],
    [sg.Frame("1. Select Data File", [[sg.Text("Data File:"), sg.Input(key='-FILE_PATH-', readonly=True, expand_x=True),
                                       sg.FileBrowse(file_types=(("Excel Files", "*.xlsx"),))]], expand_x=True)],
    [sg.Frame("2. Pre-Process Addresses (Run if you add new recipients)",
              [[sg.Button("Geocode New Addresses", key='-GEOCODE-', expand_x=True)]], expand_x=True)],
    [sg.Frame("3. Configure and Run Scheduler", [
        [sg.Text("Days Ahead:"), sg.Input(default_text='1', key='-DAYS_AHEAD-', size=(3, 1)),
         sg.Text("(1=Tomorrow [Max 3])"),
         sg.Checkbox('Use Stable Travel Times (from cache)', default=False, key='-USE_CACHE-')]], expand_x=True)],
    [sg.Button("Optimize Schedule", key='-OPTIMIZE-', expand_x=True, pad=(0, 10))],
    [sg.Frame("Process Log", [[sg.Output(size=(80, 20), key='-LOG-')]], expand_x=True, expand_y=True)]
]

window = sg.Window("WCK Delivery Scheduler", layout, resizable=True)

while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED: break

    if event == '-GEOCODE-':
        window[event].update(disabled=True)
        window.perform_long_operation(lambda: run_geocoding_process(window, values), '-GEOCODE_COMPLETE-')

    elif event == '-OPTIMIZE-':
        window[event].update(disabled=True)
        window.perform_long_operation(lambda: start_full_process(window, values), '-OPTIMIZE_COMPLETE-')

    elif event in ('-GEOCODE_COMPLETE-', '-OPTIMIZE_COMPLETE-'):
        button_key = event.split('_')[0] + '-'
        window[button_key].update(disabled=False)

window.close()