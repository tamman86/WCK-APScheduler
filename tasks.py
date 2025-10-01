import pandas as pd
import googlemaps
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, COIN_CMD
from datetime import datetime, timedelta, time
import folium
import os
import sys
import json
import time as tm

# --- CONFIGURATION ---
GEOCODE_CACHE_FILE = 'geocode_cache.json'
STABLE_COSTS_FILE = 'stable_travel_costs.csv'

def create_delivery_map(plan_df, suppliers_df, recipients_df, map_filename):
    print(f"Generating coverage map: {map_filename}")
    try:
        # Create dictionaries for quick coordinate lookup
        supplier_coords = suppliers_df.set_index('Name')[['Lat', 'Long']].to_dict('index')
        recipient_coords = recipients_df.set_index('Name')[['Latitude', 'Longitude']].to_dict('index')

        # Calculate the center of the map to focus on the delivery area
        all_lats = pd.concat([suppliers_df['Lat'], recipients_df['Latitude']]).dropna()
        all_lons = pd.concat([suppliers_df['Long'], recipients_df['Longitude']]).dropna()

        if all_lats.empty or all_lons.empty:
            print("WARNING: Cannot generate map, no coordinates found.")
            return

        map_center = [all_lats.mean(), all_lons.mean()]
        delivery_map = folium.Map(location=map_center, zoom_start=10)

        # Add markers and lines for each delivery in the plan
        for _, row in plan_df.iterrows():
            supplier_name = row['Supplier']
            recipient_name = row['Deliver_To_Recipient']

            s_info = supplier_coords.get(supplier_name)
            r_info = recipient_coords.get(recipient_name)

            if s_info and r_info and pd.notna(s_info['Lat']) and pd.notna(r_info['Latitude']):
                s_lat, s_lon = s_info['Lat'], s_info['Long']
                r_lat, r_lon = r_info['Latitude'], r_info['Longitude']

                # Add markers
                folium.Marker(
                    [s_lat, s_lon],
                    popup=f"<b>Supplier:</b> {supplier_name}",
                    icon=folium.Icon(color='blue', icon='truck', prefix='fa')
                ).add_to(delivery_map)

                folium.Marker(
                    [r_lat, r_lon],
                    popup=f"<b>Recipient:</b> {recipient_name}",
                    icon=folium.Icon(color='green', icon='home')
                ).add_to(delivery_map)

                # Add connecting line
                folium.PolyLine(
                    locations=[[s_lat, s_lon], [r_lat, r_lon]],
                    color='red', weight=2.5, opacity=0.8
                ).add_to(delivery_map)

        delivery_map.save(map_filename)
        print("Map generation complete.")
    except Exception as e:
        print(f"An error occurred during map generation: {e}")


def run_optimization_job(excel_path, api_key, days_ahead):
    """The main long-running task, adapted from the GUI script's logic."""
    try:
        # --- 1. Setup Dates and Output Filenames ---
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

        # --- 2. Load Data and Filter by Day ---
        suppliers_df = pd.read_excel(excel_path, sheet_name="Suppliers")
        recipients_df = pd.read_excel(excel_path, sheet_name="Recipients")
        available_suppliers_df = suppliers_df[
            suppliers_df['Availability'].str.contains(day_filter_str, na=False)].copy()

        if available_suppliers_df.empty:
            raise ValueError(f"No suppliers found with availability for '{day_filter_str}'.")

        # --- 3. Get Distance Matrix (Batch Processing) ---
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
        cost_df = pd.DataFrame(all_results)

        # --- 4. Run Optimization ---
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
        if getattr(sys, 'frozen', False):
            solver = COIN_CMD(path=os.path.join(sys._MEIPASS, 'cbc.exe'))

        model.solve(solver)

        if LpStatus[model.status] == 'Optimal':
            delivery_plan = [{'Supplier': s, 'Deliver_To_Recipient': r, 'NumberOfMeals': int(routes[s][r].varValue)} for
                             s in suppliers for r in recipients if routes[s][r].varValue > 0]
            plan_df = pd.DataFrame(delivery_plan)

            # --- 5. Save Results and Create Map ---
            with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
                plan_df.to_excel(writer, sheet_name=plan_sheet_name, index=False)

            create_delivery_map(plan_df, available_suppliers_df, recipients_df, output_map_path)

            # --- 6. Return the file paths ---
            return {'excel_path': output_excel_path, 'map_path': output_map_path}
        else:
            raise Exception("Optimal solution could not be found. Check if total supply can meet total demand.")

    except Exception as e:
        return {'error': str(e)}