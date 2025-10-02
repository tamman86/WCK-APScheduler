import os
import sys
import json
import uuid
import time as tm
from datetime import datetime, timedelta, time
import traceback
import pandas as pd
import googlemaps
import folium
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, COIN_CMD
from flask import Flask, request, render_template, redirect, url_for, jsonify, send_from_directory
from flask_apscheduler import APScheduler

# --- CONFIGURATION ---
GEOCODE_CACHE_FILE = 'geocode_cache.json'
STABLE_COSTS_FILE = 'stable_travel_costs.csv'


class Config:
    SCHEDULER_API_ENABLED = True


# --- HELPER FUNCTIONS ---
def load_geocode_cache():
    try:
        with open(GEOCODE_CACHE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_geocode_cache(cache_data):
    with open(GEOCODE_CACHE_FILE, 'w') as f:
        json.dump(cache_data, f, indent=4)

def create_delivery_map(plan_df, suppliers_df, recipients_df, map_filename):
    """Generates an interactive HTML map of the delivery routes with unique supplier colors."""
    try:
        # CORRECTED: Standardized to Latitude/Longitude
        supplier_coords = suppliers_df.set_index('Name')[['Latitude', 'Longitude']].to_dict('index')
        recipient_coords = recipients_df.set_index('Name')[['Latitude', 'Longitude']].to_dict('index')
        all_lats = pd.concat([suppliers_df['Latitude'], recipients_df['Latitude']]).dropna()
        all_lons = pd.concat([suppliers_df['Longitude'], recipients_df['Longitude']]).dropna()

        if all_lats.empty or all_lons.empty:
            print("WARNING: Cannot generate map, no coordinates found.")
            return

        map_center = [all_lats.mean(), all_lons.mean()]
        delivery_map = folium.Map(location=map_center, zoom_start=10)

        colors = ['red', 'blue', 'purple', 'orange', 'darkred', 'lightred', 'darkblue', 'cadetblue', 'darkpurple',
                  'pink', 'lightblue', 'green', 'gray', 'black', 'lightgreen']
        supplier_color_map = {name: colors[i % len(colors)] for i, name in enumerate(plan_df['Supplier'].unique())}

        for _, row in plan_df.iterrows():
            supplier_name, recipient_name = row['Supplier'], row['Deliver_To_Recipient']
            s_info, r_info = supplier_coords.get(supplier_name), recipient_coords.get(recipient_name)

            if s_info and r_info and pd.notna(s_info['Latitude']) and pd.notna(r_info['Latitude']):
                s_lat, s_lon = s_info['Latitude'], s_info['Longitude']
                r_lat, r_lon = r_info['Latitude'], r_info['Longitude']
                supplier_color = supplier_color_map.get(supplier_name, 'gray')

                folium.Marker([s_lat, s_lon], popup=f"<b>Supplier:</b> {supplier_name}",
                              icon=folium.Icon(color=supplier_color, icon='truck', prefix='fa')).add_to(delivery_map)
                folium.Marker([r_lat, r_lon], popup=f"<b>Recipient:</b> {recipient_name}",
                              icon=folium.Icon(color='green', icon='home')).add_to(delivery_map)
                folium.PolyLine(locations=[[s_lat, s_lon], [r_lat, r_lon]], color=supplier_color, weight=2.5,
                                opacity=0.8).add_to(delivery_map)

        delivery_map.save(map_filename)
        print(f"Map generation complete: {map_filename}")
    except Exception as e:
        print(f"Map generation failed: {e}")


# --- THE COMBINED BACKGROUND JOB ---
def run_combined_job(excel_path, api_key, days_ahead, use_stable_cache, job_id):
    """
    Performs the entire workflow: geocodes missing addresses, calculates optimal routes,
    and generates the final schedule and map.
    """
    global job_results
    job_results[job_id] = {'status': 'running'}
    try:
        # --- PART 1: GEOCODING LOGIC ---
        print("Starting Part 1: Geocoding...")
        recipients_df = pd.read_excel(excel_path, sheet_name="Recipients")
        geocode_cache = load_geocode_cache()
        new_coords_added_to_cache = False
        file_was_modified = False

        if 'Latitude' not in recipients_df.columns: recipients_df['Latitude'] = None
        if 'Longitude' not in recipients_df.columns: recipients_df['Longitude'] = None

        gmaps = googlemaps.Client(key=api_key)
        for index, row in recipients_df.iterrows():
            if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
                address = row['Full Address']
                if not isinstance(address, str) or address.strip() == '': continue

                lat, lng = None, None
                if address in geocode_cache:
                    lat, lng = geocode_cache[address]['lat'], geocode_cache[address]['lng']
                else:
                    geocode_result = gmaps.geocode(address)
                    if geocode_result:
                        location = geocode_result[0]['geometry']['location']
                        lat, lng = location['lat'], location['lng']
                        geocode_cache[address] = {'lat': lat, 'lng': lng}
                        new_coords_added_to_cache = True
                    else:
                        print(f"WARNING: Could not geocode '{address}'")

                if lat is not None and lng is not None:
                    recipients_df.at[index, 'Latitude'], recipients_df.at[index, 'Longitude'] = lat, lng
                    file_was_modified = True

        if new_coords_added_to_cache: save_geocode_cache(geocode_cache)

        if file_was_modified:
            print("Geocoding complete. Updating Excel file in place...")
            with pd.ExcelFile(excel_path) as xls:
                all_sheets = {sheet_name: xls.parse(sheet_name) for sheet_name in xls.sheet_names}
            all_sheets['Recipients'] = recipients_df
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for sheet_name, df_sheet in all_sheets.items():
                    df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            print("No new addresses needed geocoding.")

        # --- PART 2: OPTIMIZATION LOGIC ---
        print("\nStarting Part 2: Optimization...")
        today = datetime.now()
        future_date = today + timedelta(days=days_ahead)
        departure_datetime = datetime.combine(future_date.date(), time(11, 0))
        departure_timestamp = int(departure_datetime.timestamp())
        day_filter_str = future_date.strftime('%a').replace("Tue", "Tues")
        day_full_name = future_date.strftime('%A')
        formatted_date = future_date.strftime('%Y-%m-%d')
        sheet_title = f"Delivery Schedule for {day_full_name}, {formatted_date}"

        timestamp = today.strftime('%Y%m%d-%H%M%S')
        plan_sheet_name = f"DeliveryPlan_{days_ahead}-Day"
        output_excel_path = os.path.join('outputs', f"{timestamp}_Schedule.xlsx")
        output_map_path = os.path.join('outputs', f"{timestamp}_Map.html")

        suppliers_df = pd.read_excel(excel_path, sheet_name="Suppliers")
        # Reread the recipients_df from the (now updated) file
        recipients_df = pd.read_excel(excel_path, sheet_name="Recipients")
        available_suppliers_df = suppliers_df[
            suppliers_df['Availability'].str.contains(day_filter_str, na=False)].copy()

        if available_suppliers_df.empty:
            raise ValueError(f"No suppliers found with availability for '{day_filter_str}'.")

        if use_stable_cache:
            try:
                cost_df = pd.read_csv(STABLE_COSTS_FILE)
            except FileNotFoundError:
                raise ValueError(
                    f"Cache file '{STABLE_COSTS_FILE}' not found. Please run once in normal mode to create it.")
        else:
            all_recipient_coords = list(zip(recipients_df['Latitude'], recipients_df['Longitude']))
            all_supplier_coords = list(zip(available_suppliers_df['Latitude'], available_suppliers_df['Longitude']))
            all_results = []
            batch_size = 10

            for i in range(0, len(available_suppliers_df), batch_size):
                supplier_batch_df = available_suppliers_df.iloc[i:i + batch_size]
                supplier_batch_coords = all_supplier_coords[i:i + batch_size]
                for j in range(0, len(recipients_df), batch_size):
                    recipient_batch_df = recipients_df.iloc[j:j + batch_size]
                    recipient_batch_coords = all_recipient_coords[j:j + batch_size]
                    matrix_result = gmaps.distance_matrix(origins=supplier_batch_coords,
                                                          destinations=recipient_batch_coords, mode="driving",
                                                          departure_time=departure_timestamp)
                    for supplier_idx, row in enumerate(matrix_result['rows']):
                        for recipient_idx, element in enumerate(row['elements']):
                            if element['status'] == 'OK':
                                all_results.append({'Supplier': supplier_batch_df.iloc[supplier_idx]['Name'],
                                                    'Recipient': recipient_batch_df.iloc[recipient_idx]['Name'],
                                                    'TravelTime_Minutes': round(element['duration']['value'] / 60, 2)})
                    tm.sleep(1)

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
                final_cache_df = merged_df[['Supplier', 'Recipient', 'TravelTime_Minutes']]
            except FileNotFoundError:
                final_cache_df = new_cost_df

            final_cache_df.to_csv(STABLE_COSTS_FILE, index=False)
            cost_df = new_cost_df

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

        model.solve(None)

        if LpStatus[model.status] == 'Optimal':
            delivery_plan = [{'Supplier': s, 'Deliver_To_Recipient': r, 'NumberOfMeals': int(routes[s][r].varValue)} for
                             s in suppliers for r in recipients if routes[s][r].varValue > 0]
            plan_df = pd.DataFrame(delivery_plan)

            with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
                plan_df.to_excel(writer, sheet_name=plan_sheet_name, index=False)

            create_delivery_map(plan_df, available_suppliers_df, recipients_df, output_map_path)

            result = {'excel_path': output_excel_path, 'map_path': output_map_path}
            job_results[job_id]['status'] = 'finished'
            job_results[job_id]['result'] = result
        else:
            raise Exception("Optimal solution could not be found.")

    except Exception as e:
        print("--- JOB FAILED: FULL TRACEBACK ---")
        traceback.print_exc()
        print("------------------------------------")
        job_results[job_id]['status'] = 'failed'
        job_results[job_id]['result'] = {'error': str(e)}

# --- FLASK APP INITIALIZATION ---
app = Flask(__name__)
app.config.from_object(Config())
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs('outputs', exist_ok=True)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

job_results = {}


# --- FLASK ROUTES ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run', methods=['POST'])
def run_task():
    # This is now the only "run" route
    if 'data_file' not in request.files or request.files['data_file'].filename == '':
        return "Error: No file selected.", 400
    file = request.files['data_file']
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)
    api_key = request.form['api_key']
    days_ahead = int(request.form['days_ahead'])
    use_stable_cache = 'use_cache' in request.form
    job_id = str(uuid.uuid4())
    job_results[job_id] = {'status': 'queued'}

    # Schedule the new combined job
    scheduler.add_job(id=job_id, func=run_combined_job, trigger='date',
                      args=[filepath, api_key, days_ahead, use_stable_cache, job_id])

    return redirect(url_for('results', job_id=job_id))

@app.route('/results/<job_id>')
def results(job_id):
    return render_template('results.html', job_id=job_id)


@app.route('/status/<job_id>')
def job_status(job_id):
    return jsonify(job_results.get(job_id, {'status': 'not_found'}))


@app.route('/outputs/<filename>')
def serve_output_file(filename):
    return send_from_directory('outputs', filename)


# --- This section is needed to run the app locally ---
if __name__ == '__main__':
    app.run(debug=True, port=5001)