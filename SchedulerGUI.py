import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog
import threading
import sys
import os
import json
import time as tm
from datetime import datetime, timedelta, time

# --- YOUR LOGIC IMPORTS ---
import pandas as pd
import googlemaps
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, COIN_CMD
import folium

# --- CONFIGURATION ---
GEOCODE_CACHE_FILE = 'geocode_cache.json'
STABLE_COSTS_FILE = 'stable_travel_costs.csv'


# --- LOGIC FUNCTIONS ---

def load_geocode_cache():
    try:
        with open(GEOCODE_CACHE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_geocode_cache(cache_data):
    with open(GEOCODE_CACHE_FILE, 'w') as f:
        json.dump(cache_data, f, indent=4)


def create_delivery_map(plan_df, suppliers_df, recipients_df, map_filename, log_func):
    log_func(f"Generating coverage map: {map_filename}")
    try:
        supplier_coords = suppliers_df.set_index('Name')[['Lat', 'Long']].to_dict('index')
        recipient_coords = recipients_df.set_index('Name')[['Latitude', 'Longitude']].to_dict('index')

        all_lats = pd.concat([suppliers_df['Lat'], recipients_df['Latitude']]).dropna()
        all_lons = pd.concat([suppliers_df['Long'], recipients_df['Longitude']]).dropna()

        if all_lats.empty or all_lons.empty:
            log_func("WARNING: Cannot generate map, no coordinates found.")
            return

        map_center = [all_lats.mean(), all_lons.mean()]
        delivery_map = folium.Map(location=map_center, zoom_start=10)

        for _, row in plan_df.iterrows():
            supplier_name = row['Supplier']
            recipient_name = row['Deliver_To_Recipient']

            s_info = supplier_coords.get(supplier_name)
            r_info = recipient_coords.get(recipient_name)

            if s_info and r_info and pd.notna(s_info['Lat']) and pd.notna(r_info['Latitude']):
                s_lat, s_lon = s_info['Lat'], s_info['Long']
                r_lat, r_lon = r_info['Latitude'], r_info['Longitude']

                folium.Marker([s_lat, s_lon], popup=f"Supplier: {supplier_name}",
                              icon=folium.Icon(color='blue', icon='truck', prefix='fa')).add_to(delivery_map)
                folium.Marker([r_lat, r_lon], popup=f"Recipient: {recipient_name}",
                              icon=folium.Icon(color='green', icon='home')).add_to(delivery_map)
                folium.PolyLine(locations=[[s_lat, s_lon], [r_lat, r_lon]], color='red', weight=2.5,
                                opacity=0.8).add_to(delivery_map)

        delivery_map.save(map_filename)
        log_func("Map generation complete.")
    except Exception as e:
        log_func(f"An error occurred during map generation: {e}")


def run_geocoding_logic(values, log_func):
    log_func("--- Starting Geocoding Pre-Process ---")
    excel_path = values['file_path']
    api_key = values['api_key']

    if not api_key:
        log_func("ERROR: Please enter your Google Maps API key.\n")
        return
    if not excel_path:
        log_func("ERROR: Please select a data file first.\n")
        return

    try:
        df = pd.read_excel(excel_path, sheet_name="Recipients")
        geocode_cache = load_geocode_cache()
        new_coords_added = False

        if 'Latitude' not in df.columns: df['Latitude'] = None
        if 'Longitude' not in df.columns: df['Longitude'] = None

        gmaps = googlemaps.Client(key=api_key)

        for index, row in df.iterrows():
            if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
                address = row['Full Address']
                if not isinstance(address, str) or address.strip() == '': continue

                if address in geocode_cache:
                    log_func(f"CACHE HIT: Found coordinates for '{address}'")
                    lat, lng = geocode_cache[address]['lat'], geocode_cache[address]['lng']
                else:
                    log_func(f"API CALL: Geocoding new address '{address}'...")
                    geocode_result = gmaps.geocode(address)
                    if geocode_result:
                        location = geocode_result[0]['geometry']['location']
                        lat, lng = location['lat'], location['lng']
                        geocode_cache[address] = {'lat': lat, 'lng': lng}
                        new_coords_added = True
                    else:
                        log_func(f"WARNING: Could not geocode '{address}'")
                        lat, lng = None, None

                df.at[index, 'Latitude'] = lat
                df.at[index, 'Longitude'] = lng

        if new_coords_added:
            save_geocode_cache(geocode_cache)
            log_func("Saving updated coordinates back to the 'Recipients' sheet...")
            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name="Recipients", index=False)
        else:
            log_func("No new addresses found to geocode.")

        log_func("--- Geocoding Pre-Process Complete ---\n")
    except Exception as e:
        log_func(f"\n--- AN ERROR OCCURRED DURING GEOCODING ---")
        log_func(str(e))


def run_optimization_logic(values, log_func):
    try:
        excel_path = values['file_path']
        use_stable_cache = values['use_cache']
        api_key = values['api_key']
        days_ahead = int(values['days_ahead'])

        if not api_key:
            log_func("ERROR: Please enter your Google Maps API key.\n")
            return
        if not excel_path:
            log_func("ERROR: Please select a data file first.\n")
            return

        log_func("--- Starting Optimization Process ---\n")

        today = datetime.now()
        future_date = today + timedelta(days=days_ahead)
        departure_datetime = datetime.combine(future_date.date(), time(11, 0))
        departure_timestamp = int(departure_datetime.timestamp())
        day_filter_str = future_date.strftime('%a').replace("Tue", "Tues")
        day_full_name = future_date.strftime('%A')
        formatted_date = future_date.strftime('%Y-%m-%d')
        sheet_title = f"Delivery Schedule for {day_full_name}, {formatted_date}"

        log_func(f"Forecasting for {days_ahead}-day(s) ahead: {day_full_name}")

        suppliers_df = pd.read_excel(excel_path, sheet_name="Suppliers")
        recipients_df = pd.read_excel(excel_path, sheet_name="Recipients")

        available_suppliers_df = suppliers_df[
            suppliers_df['Availability'].str.contains(day_filter_str, na=False)].copy()
        if available_suppliers_df.empty:
            log_func(f"ERROR: No suppliers found with availability for '{day_filter_str}'.\n")
            return

        log_func(f"Found {len(available_suppliers_df)} available suppliers and {len(recipients_df)} recipients.\n")

        cost_df = None

        if use_stable_cache:
            log_func("Step 2: Using stable travel times from cache file...")
            try:
                cost_df = pd.read_csv(STABLE_COSTS_FILE)
                log_func("Successfully loaded travel times from cache.\n")
            except FileNotFoundError:
                log_func(f"ERROR: Cache file '{STABLE_COSTS_FILE}' not found. Run once in normal mode first.\n")
                return
        else:
            log_func(
                f"Step 2: Requesting predictive travel data for {departure_datetime.strftime('%Y-%m-%d %I:%M %p')}...")
            gmaps = googlemaps.Client(key=api_key)
            all_recipient_coords = list(zip(recipients_df['Latitude'], recipients_df['Longitude']))
            all_supplier_coords = list(zip(available_suppliers_df['Lat'], available_suppliers_df['Long']))
            all_results = []
            batch_size = 10

            for i in range(0, len(available_suppliers_df), batch_size):
                supplier_batch_df = available_suppliers_df.iloc[i:i + batch_size]
                supplier_batch_coords = all_supplier_coords[i:i + batch_size]
                for j in range(0, len(recipients_df), batch_size):
                    recipient_batch_df = recipients_df.iloc[j:j + batch_size]
                    recipient_batch_coords = all_recipient_coords[j:j + batch_size]

                    log_func(f"  - Processing batch S:{i // batch_size + 1} vs R:{j // batch_size + 1}...")

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
                    tm.sleep(1)

            log_func("\nProcessing travel data and updating stable cache file...\n")
            new_cost_df = pd.DataFrame(all_results)

            # Cache merging logic
            try:
                cached_cost_df = pd.read_csv(STABLE_COSTS_FILE)
                merged_df = pd.merge(new_cost_df, cached_cost_df, on=['Supplier', 'Recipient'], how='left',
                                     suffixes=('_new', '_cached'))

                def get_best_time(row):
                    if pd.notna(row['TravelTime_Minutes_cached']):
                        return min(row['TravelTime_Minutes_new'], row['TravelTime_Minutes_cached'])
                    return row['TravelTime_Minutes_new']

                merged_df['TravelTime_Minutes'] = merged_df.apply(get_best_time, axis=1)
                cost_df = new_cost_df
                final_cache_df = merged_df[['Supplier', 'Recipient', 'TravelTime_Minutes']]
            except FileNotFoundError:
                log_func("No existing cache file found. Creating a new one.")
                cost_df = new_cost_df
                final_cache_df = new_cost_df

            final_cache_df.to_csv(STABLE_COSTS_FILE, index=False)
            log_func("Stable travel cost file updated.")

        log_func("Step 3: Formulating optimization problem...")
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

        # Solver Path Logic
        solver = None
        if getattr(sys, 'frozen', False):
            solver_path = os.path.join(sys._MEIPASS, 'cbc.exe')
            solver = COIN_CMD(path=solver_path)
        else:
            if sys.platform == "darwin":
                if os.path.exists('/opt/homebrew/bin/cbc'):
                    solver = COIN_CMD(path='/opt/homebrew/bin/cbc')
                elif os.path.exists('/usr/local/bin/cbc'):
                    solver = COIN_CMD(path='/usr/local/bin/cbc')

        model.solve(solver)
        log_func(f"Solver status: {LpStatus[model.status]}\n")

        if LpStatus[model.status] == 'Optimal':
            plan_sheet_name = f"DeliveryPlan_{days_ahead}-Day"
            log_func(f"Step 4: Optimal solution found! Saving to '{plan_sheet_name}'...")

            delivery_plan = [{'Supplier': s, 'Deliver_To_Recipient': r, 'NumberOfMeals': int(routes[s][r].varValue)}
                             for s in suppliers for r in recipients if routes[s][r].varValue > 0]
            plan_df = pd.DataFrame(delivery_plan)

            title_df = pd.DataFrame([sheet_title] + [None] * (len(plan_df.columns) - 1)).T
            title_df.columns = plan_df.columns
            final_df = pd.concat([title_df, plan_df], ignore_index=True)

            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                final_df.to_excel(writer, sheet_name=plan_sheet_name, index=False, header=[None] * len(plan_df.columns))
                plan_df.to_excel(writer, sheet_name=plan_sheet_name, startrow=2, index=False)

            log_func(f"Schedule saved to '{plan_sheet_name}'.")

            log_func("\nStep 5: Generating delivery coverage map...")
            map_filename = f"DeliveryMap_{days_ahead}-Day.html"
            create_delivery_map(plan_df, available_suppliers_df, recipients_df, map_filename, log_func)
            log_func("\n--- PROCESS COMPLETE ---")
        else:
            log_func("\n--- PROCESS FAILED ---")
            log_func("Could not find optimal solution. Check supply vs demand.")

    except Exception as e:
        log_func(f"\n--- AN ERROR OCCURRED DURING OPTIMIZATION ---")
        log_func(str(e))


# --- GUI CLASS ---

ctk.set_appearance_mode("Dark")  # Modes: "System" (standard), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"


class SchedulerApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("WCK Delivery Scheduler")
        self.geometry("850x650")

        # Layout config
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)  # Log area expands

        self.setup_ui()

    def setup_ui(self):
        # --- 1. Configuration Frame ---
        self.config_frame = ctk.CTkFrame(self)
        self.config_frame.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="ew")
        self.config_frame.grid_columnconfigure(1, weight=1)

        # API Key
        ctk.CTkLabel(self.config_frame, text="Google Maps API Key:").grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.api_entry = ctk.CTkEntry(self.config_frame, show="*", placeholder_text="Enter API Key here")
        self.api_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")

        # File Selection
        ctk.CTkLabel(self.config_frame, text="Data File (Excel):").grid(row=1, column=0, padx=10, pady=10, sticky="w")
        self.file_entry = ctk.CTkEntry(self.config_frame, placeholder_text="No file selected")
        self.file_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")

        self.browse_btn = ctk.CTkButton(self.config_frame, text="Browse", command=self.browse_file, width=100)
        self.browse_btn.grid(row=1, column=2, padx=10, pady=10)

        # --- 2. Action Frame ---
        self.action_frame = ctk.CTkFrame(self)
        self.action_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.action_frame.grid_columnconfigure((0, 1), weight=1)

        # Left Side: Geocoding
        self.geo_frame = ctk.CTkFrame(self.action_frame)
        self.geo_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        ctk.CTkLabel(self.geo_frame, text="Pre-Process Addresses", font=("Roboto", 14, "bold")).pack(pady=(10, 5))
        ctk.CTkLabel(self.geo_frame, text="(Run if adding new recipients)").pack(pady=(0, 10))
        self.geo_btn = ctk.CTkButton(self.geo_frame, text="Geocode New Addresses", fg_color="transparent",
                                     border_width=2, command=self.start_geocode_thread)
        self.geo_btn.pack(pady=10, padx=20, fill="x")

        # Right Side: Optimization
        self.opt_frame = ctk.CTkFrame(self.action_frame)
        self.opt_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")

        ctk.CTkLabel(self.opt_frame, text="Run Scheduler", font=("Roboto", 14, "bold")).pack(pady=(10, 5))

        # Controls row
        self.ctrl_inner = ctk.CTkFrame(self.opt_frame, fg_color="transparent")
        self.ctrl_inner.pack(pady=5)

        ctk.CTkLabel(self.ctrl_inner, text="Days Ahead:").pack(side="left", padx=5)
        self.days_entry = ctk.CTkEntry(self.ctrl_inner, width=40)
        self.days_entry.insert(0, "1")
        self.days_entry.pack(side="left", padx=5)

        self.cache_chk = ctk.CTkCheckBox(self.ctrl_inner, text="Use Cached Travel Times")
        self.cache_chk.pack(side="left", padx=15)

        self.run_btn = ctk.CTkButton(self.opt_frame, text="Optimize Schedule", command=self.start_optimize_thread)
        self.run_btn.pack(pady=10, padx=20, fill="x")

        # --- 3. Logging Frame ---
        self.log_frame = ctk.CTkFrame(self)
        self.log_frame.grid(row=2, column=0, padx=20, pady=(10, 20), sticky="nsew")
        self.log_frame.grid_columnconfigure(0, weight=1)
        self.log_frame.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(self.log_frame, text="Process Log").grid(row=0, column=0, sticky="w", padx=10, pady=(5, 0))

        self.log_box = ctk.CTkTextbox(self.log_frame, font=("Consolas", 12))
        self.log_box.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
        self.log_box.configure(state="disabled")  # Read-only initially

    # --- Helper Functions ---
    def browse_file(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if filename:
            self.file_entry.delete(0, "end")
            self.file_entry.insert(0, filename)

    def log(self, message):
        # This function is thread-safe way to update GUI from background threads
        self.after(0, self._append_log, message)

    def _append_log(self, message):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", str(message) + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def get_current_values(self):
        return {
            'api_key': self.api_entry.get(),
            'file_path': self.file_entry.get(),
            'days_ahead': self.days_entry.get(),
            'use_cache': bool(self.cache_chk.get())
        }

    def lock_ui(self, locking=True):
        state = "disabled" if locking else "normal"
        self.geo_btn.configure(state=state)
        self.run_btn.configure(state=state)
        self.browse_btn.configure(state=state)

    # --- Threading Wrappers ---
    def start_geocode_thread(self):
        self.lock_ui(True)
        threading.Thread(target=self._run_geocode, daemon=True).start()

    def _run_geocode(self):
        vals = self.get_current_values()
        run_geocoding_logic(vals, self.log)
        self.after(0, lambda: self.lock_ui(False))

    def start_optimize_thread(self):
        self.lock_ui(True)
        threading.Thread(target=self._run_optimize, daemon=True).start()

    def _run_optimize(self):
        vals = self.get_current_values()
        run_optimization_logic(vals, self.log)
        self.after(0, lambda: self.lock_ui(False))


if __name__ == "__main__":
    app = SchedulerApp()
    app.mainloop()