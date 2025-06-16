from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import json
import logging
from datetime import datetime
import tempfile
import zipfile
from werkzeug.utils import secure_filename
import requests
from bs4 import BeautifulSoup
import time
import random
import sqlite3
from pathlib import Path
import PyPDF2
import pdfplumber
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import threading
import queue

# Initialize Flask app
app = Flask(__name__)
CORS(app, origins="*")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'
DATABASE_FILE = 'ni43101_data.db'
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# Initialize database
def init_database():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            company TEXT,
            state TEXT,
            county TEXT,
            commodities TEXT,
            report_date TEXT,
            report_type TEXT,
            coordinates_lat REAL,
            coordinates_lon REAL,
            township TEXT,
            section TEXT,
            resource_indicated_tonnage REAL,
            resource_indicated_grade TEXT,
            resource_indicated_ounces INTEGER,
            resource_inferred_tonnage REAL,
            resource_inferred_grade TEXT,
            resource_inferred_ounces INTEGER,
            risk_overall INTEGER,
            risk_technical INTEGER,
            risk_regulatory INTEGER,
            risk_market INTEGER,
            risk_infrastructure INTEGER,
            estimated_value TEXT,
            attractiveness_score REAL,
            roi TEXT,
            payback_period TEXT,
            pdf_filename TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scraping_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            company TEXT,
            state TEXT,
            commodities TEXT,
            resource_grade TEXT,
            tonnage TEXT,
            coordinates_lat REAL,
            coordinates_lon REAL,
            risk_score INTEGER,
            source_url TEXT,
            last_updated TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Initialize database on startup
init_database()

# Sample data for demonstration
SAMPLE_COMPANIES = [
    "Barrick Gold Corporation",
    "Newmont Corporation", 
    "Freeport-McMoRan Inc",
    "Nevada Gold Mines",
    "Kinross Gold Corporation",
    "Agnico Eagle Mines",
    "Coeur Mining Inc",
    "Hecla Mining Company"
]

WESTERN_STATES = [
    'Nevada', 'Arizona', 'Utah', 'Colorado', 'New Mexico', 
    'California', 'Idaho', 'Montana', 'Wyoming'
]

COMMODITIES = ['Gold', 'Silver', 'Copper', 'Lithium', 'Molybdenum', 'Lead', 'Zinc', 'Uranium']

# PDF Processing Functions
class PDFProcessor:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="choraquest_ni43101")
    
    def extract_text_from_pdf(self, pdf_path):
        """Extract text from PDF using multiple methods"""
        text = ""
        
        try:
            # Try pdfplumber first (better for tables and structured data)
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            logger.warning(f"pdfplumber failed: {e}")
            
            # Fallback to PyPDF2
            try:
                with open(pdf_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n"
            except Exception as e2:
                logger.error(f"Both PDF extraction methods failed: {e2}")
                return None
        
        return text if text.strip() else None
    
    def extract_project_metadata(self, text):
        """Extract basic project information from text"""
        metadata = {
            'project_name': None,
            'company': None,
            'state': None,
            'county': None,
            'commodities': [],
            'report_date': None,
            'report_type': None
        }
        
        # Extract project name (look for common patterns)
        project_patterns = [
            r'(?:Project|Property|Mine|Deposit)\s*:?\s*([A-Z][A-Za-z\s]+?)(?:\n|,|\.)',
            r'([A-Z][A-Za-z\s]+?)\s+(?:Project|Property|Mine|Deposit)',
            r'Technical Report.*?([A-Z][A-Za-z\s]+?)(?:Project|Property|Mine)'
        ]
        
        for pattern in project_patterns:
            match = re.search(pattern, text[:2000], re.IGNORECASE)
            if match:
                metadata['project_name'] = match.group(1).strip()
                break
        
        # Extract company name
        company_patterns = [
            r'(?:Prepared for|Client|Company)\s*:?\s*([A-Z][A-Za-z\s&.,]+?)(?:\n|,)',
            r'([A-Z][A-Za-z\s&.,]+?)\s+(?:Inc|Corp|Corporation|Ltd|Limited)',
        ]
        
        for pattern in company_patterns:
            match = re.search(pattern, text[:2000], re.IGNORECASE)
            if match:
                metadata['company'] = match.group(1).strip()
                break
        
        # Extract state
        for state in WESTERN_STATES:
            if state.lower() in text.lower():
                metadata['state'] = state
                break
        
        # Extract commodities
        for commodity in COMMODITIES:
            if commodity.lower() in text.lower():
                metadata['commodities'].append(commodity)
        
        # Extract report date
        date_patterns = [
            r'(?:Date|Effective Date)\s*:?\s*(\w+\s+\d{1,2},?\s+\d{4})',
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
            r'(\w+\s+\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text[:2000])
            if match:
                metadata['report_date'] = match.group(1)
                break
        
        return metadata
    
    def extract_resource_estimates(self, text):
        """Extract resource estimates from text"""
        resources = {
            'indicated': {'tonnage': None, 'grade': None, 'ounces': None},
            'inferred': {'tonnage': None, 'grade': None, 'ounces': None},
            'total': {'tonnage': None, 'grade': None, 'ounces': None}
        }
        
        # Look for resource tables and estimates
        resource_patterns = [
            r'Indicated.*?(\d+\.?\d*)\s*(?:million|M)\s*(?:tons|tonnes).*?(\d+\.?\d*)\s*(?:g/t|oz/ton).*?(\d+\.?\d*)\s*(?:thousand|K|million|M)\s*(?:oz|ounces)',
            r'Inferred.*?(\d+\.?\d*)\s*(?:million|M)\s*(?:tons|tonnes).*?(\d+\.?\d*)\s*(?:g/t|oz/ton).*?(\d+\.?\d*)\s*(?:thousand|K|million|M)\s*(?:oz|ounces)'
        ]
        
        # This is a simplified extraction - in practice, you'd need more sophisticated parsing
        # For demo purposes, we'll return sample data
        resources['indicated'] = {'tonnage': 2.4, 'grade': '2.1 g/t Au', 'ounces': 162000}
        resources['inferred'] = {'tonnage': 1.8, 'grade': '1.8 g/t Au', 'ounces': 104000}
        resources['total'] = {'tonnage': 4.2, 'grade': '2.0 g/t Au', 'ounces': 266000}
        
        return resources
    
    def extract_location_data(self, text):
        """Extract location and coordinate information"""
        location = {
            'coordinates': {'lat': None, 'lon': None},
            'township': None,
            'section': None
        }
        
        # Look for coordinate patterns
        coord_patterns = [
            r'(\d{2}\.\d+)°?\s*N.*?(\d{2,3}\.\d+)°?\s*W',
            r'Latitude:\s*(\d{2}\.\d+).*?Longitude:\s*(-?\d{2,3}\.\d+)',
            r'UTM.*?(\d+)\s*E.*?(\d+)\s*N'
        ]
        
        for pattern in coord_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                lat, lon = match.groups()
                location['coordinates']['lat'] = float(lat)
                location['coordinates']['lon'] = -abs(float(lon))  # Ensure western longitude is negative
                break
        
        # Look for township/range/section
        township_pattern = r'(T\d+[NS])\s*(R\d+[EW])'
        match = re.search(township_pattern, text, re.IGNORECASE)
        if match:
            location['township'] = f"{match.group(1)} {match.group(2)}"
        
        section_pattern = r'Section\s*(\d+)'
        match = re.search(section_pattern, text, re.IGNORECASE)
        if match:
            location['section'] = f"Section {match.group(1)}"
        
        return location
    
    def calculate_risk_assessment(self, metadata, resources, location):
        """Calculate risk scores based on various factors"""
        risk = {
            'overall': 5,
            'technical': 5,
            'regulatory': 5,
            'market': 5,
            'infrastructure': 5
        }
        
        # Adjust risk based on state (regulatory environment)
        state_risk = {
            'Nevada': 3, 'Arizona': 4, 'Utah': 4, 'Colorado': 5,
            'California': 7, 'Idaho': 4, 'Montana': 5, 'Wyoming': 4, 'New Mexico': 5
        }
        
        if metadata.get('state') in state_risk:
            risk['regulatory'] = state_risk[metadata['state']]
        
        # Adjust based on commodity
        commodity_risk = {'Gold': 4, 'Silver': 5, 'Copper': 3, 'Lithium': 6}
        if metadata.get('commodities'):
            for commodity in metadata['commodities']:
                if commodity in commodity_risk:
                    risk['market'] = commodity_risk[commodity]
                    break
        
        # Calculate overall risk
        risk['overall'] = int(sum(risk.values()) / len(risk))
        
        return risk
    
    def calculate_investment_metrics(self, resources, risk):
        """Calculate investment attractiveness metrics"""
        metrics = {
            'estimated_value': '$45.2M',
            'attractiveness_score': 7.5,
            'roi': '18-24%',
            'payback_period': '3.2 years'
        }
        
        # Adjust based on resource size and risk
        if resources['total']['ounces']:
            ounces = resources['total']['ounces']
            if ounces > 500000:
                metrics['attractiveness_score'] = 8.5
                metrics['estimated_value'] = f"${ounces * 0.0002:.1f}M"
            elif ounces > 100000:
                metrics['attractiveness_score'] = 7.0
                metrics['estimated_value'] = f"${ounces * 0.00015:.1f}M"
        
        # Adjust for risk
        risk_adjustment = (10 - risk['overall']) / 10
        metrics['attractiveness_score'] *= risk_adjustment
        
        return metrics

# Scraping Functions
class WebScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def scrape_sedar_plus(self, states, max_results=10):
        """Simulate SEDAR+ scraping (in practice, this would use Selenium)"""
        results = []
        
        # Simulate scraping delay
        time.sleep(2)
        
        # Generate sample results
        for i in range(max_results):
            state = random.choice(states)
            company = random.choice(SAMPLE_COMPANIES)
            commodities = random.sample(COMMODITIES, random.randint(1, 3))
            
            result = {
                'project_name': f"{random.choice(['Golden', 'Silver', 'Copper', 'Eagle', 'Mountain', 'Valley'])} {random.choice(['Creek', 'Ridge', 'Peak', 'Mine', 'Deposit'])}",
                'company': company,
                'state': state,
                'commodities': commodities,
                'resource_grade': f"{random.uniform(0.5, 3.0):.1f} g/t Au" if 'Gold' in commodities else f"{random.uniform(0.2, 1.5):.2f}% Cu",
                'tonnage': f"{random.uniform(10, 500):.0f}M tons",
                'coordinates_lat': random.uniform(32.0, 49.0),
                'coordinates_lon': random.uniform(-125.0, -104.0),
                'risk_score': random.randint(2, 8),
                'source_url': f"https://sedarplus.ca/report_{i+1}",
                'last_updated': datetime.now().strftime('%Y-%m-%d')
            }
            
            results.append(result)
            
            # Simulate progressive results
            yield result
            time.sleep(1)
        
        return results
    
    def scrape_company_websites(self, companies, max_results=5):
        """Simulate company website scraping"""
        results = []
        
        for company in companies[:max_results]:
            time.sleep(1)  # Simulate scraping delay
            
            state = random.choice(WESTERN_STATES)
            commodities = random.sample(COMMODITIES, random.randint(1, 2))
            
            result = {
                'project_name': f"{company.split()[0]} {random.choice(['Project', 'Mine', 'Property'])}",
                'company': company,
                'state': state,
                'commodities': commodities,
                'resource_grade': f"{random.uniform(1.0, 4.0):.1f} g/t Au" if 'Gold' in commodities else f"{random.uniform(0.3, 2.0):.2f}% Cu",
                'tonnage': f"{random.uniform(50, 1000):.0f}M tons",
                'coordinates_lat': random.uniform(32.0, 49.0),
                'coordinates_lon': random.uniform(-125.0, -104.0),
                'risk_score': random.randint(1, 7),
                'source_url': f"https://{company.lower().replace(' ', '')}.com/projects",
                'last_updated': datetime.now().strftime('%Y-%m-%d')
            }
            
            results.append(result)
            yield result

# API Routes
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/api/upload', methods=['POST'])
def upload_files():
    """Upload and process PDF files"""
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files provided'}), 400
        
        files = request.files.getlist('files')
        if not files or files[0].filename == '':
            return jsonify({'error': 'No files selected'}), 400
        
        uploaded_files = []
        
        for file in files:
            if file and file.filename.lower().endswith('.pdf'):
                # Check file size
                file.seek(0, os.SEEK_END)
                file_size = file.tell()
                file.seek(0)
                
                if file_size > MAX_FILE_SIZE:
                    return jsonify({'error': f'File {file.filename} is too large (max 50MB)'}), 400
                
                # Save file
                filename = secure_filename(file.filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                unique_filename = f"{timestamp}_{filename}"
                file_path = os.path.join(UPLOAD_FOLDER, unique_filename)
                
                file.save(file_path)
                
                uploaded_files.append({
                    'filename': filename,
                    'unique_filename': unique_filename,
                    'size': file_size,
                    'path': file_path
                })
        
        return jsonify({
            'message': f'Successfully uploaded {len(uploaded_files)} files',
            'files': uploaded_files
        })
    
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'error': 'Upload failed'}), 500

@app.route('/api/process', methods=['POST'])
def process_files():
    """Process uploaded PDF files"""
    try:
        data = request.get_json()
        filenames = data.get('filenames', [])
        
        if not filenames:
            return jsonify({'error': 'No files to process'}), 400
        
        processor = PDFProcessor()
        results = []
        
        for filename in filenames:
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            
            if not os.path.exists(file_path):
                continue
            
            # Extract text
            text = processor.extract_text_from_pdf(file_path)
            if not text:
                continue
            
            # Extract information
            metadata = processor.extract_project_metadata(text)
            resources = processor.extract_resource_estimates(text)
            location = processor.extract_location_data(text)
            risk = processor.calculate_risk_assessment(metadata, resources, location)
            investment = processor.calculate_investment_metrics(resources, risk)
            
            # Combine results
            result = {
                'filename': filename,
                'project_overview': metadata,
                'resource_estimate': resources,
                'location': location,
                'risk_assessment': risk,
                'investment_metrics': investment
            }
            
            # Save to database
            save_project_to_db(result)
            
            results.append(result)
        
        return jsonify({
            'message': f'Successfully processed {len(results)} files',
            'results': results
        })
    
    except Exception as e:
        logger.error(f"Processing error: {e}")
        return jsonify({'error': 'Processing failed'}), 500

@app.route('/api/scrape/start', methods=['POST'])
def start_scraping():
    """Start regional scraping process"""
    try:
        data = request.get_json()
        states = data.get('states', WESTERN_STATES)
        sources = data.get('sources', ['sedar', 'companies'])
        
        # Start scraping in background thread
        scraping_thread = threading.Thread(
            target=background_scraping,
            args=(states, sources)
        )
        scraping_thread.daemon = True
        scraping_thread.start()
        
        return jsonify({
            'message': 'Scraping started',
            'states': states,
            'sources': sources
        })
    
    except Exception as e:
        logger.error(f"Scraping start error: {e}")
        return jsonify({'error': 'Failed to start scraping'}), 500

@app.route('/api/scrape/results', methods=['GET'])
def get_scraping_results():
    """Get current scraping results"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM scraping_results 
            ORDER BY created_at DESC 
            LIMIT 50
        ''')
        
        columns = [description[0] for description in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        conn.close()
        
        return jsonify({
            'results': results,
            'count': len(results)
        })
    
    except Exception as e:
        logger.error(f"Get results error: {e}")
        return jsonify({'error': 'Failed to get results'}), 500

@app.route('/api/projects', methods=['GET'])
def get_projects():
    """Get all processed projects"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM projects 
            ORDER BY created_at DESC
        ''')
        
        columns = [description[0] for description in cursor.description]
        projects = []
        
        for row in cursor.fetchall():
            project = dict(zip(columns, row))
            projects.append(project)
        
        conn.close()
        
        return jsonify({
            'projects': projects,
            'count': len(projects)
        })
    
    except Exception as e:
        logger.error(f"Get projects error: {e}")
        return jsonify({'error': 'Failed to get projects'}), 500

@app.route('/api/map/data', methods=['GET'])
def get_map_data():
    """Get data for map visualization"""
    try:
        view_type = request.args.get('view', 'projects')
        
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        if view_type == 'projects':
            cursor.execute('''
                SELECT project_name, company, state, coordinates_lat, coordinates_lon, 
                       risk_overall, commodities, estimated_value
                FROM projects 
                WHERE coordinates_lat IS NOT NULL AND coordinates_lon IS NOT NULL
            ''')
        else:
            cursor.execute('''
                SELECT project_name, company, state, coordinates_lat, coordinates_lon, 
                       risk_score, commodities
                FROM scraping_results 
                WHERE coordinates_lat IS NOT NULL AND coordinates_lon IS NOT NULL
            ''')
        
        columns = [description[0] for description in cursor.description]
        data = []
        
        for row in cursor.fetchall():
            item = dict(zip(columns, row))
            data.append(item)
        
        conn.close()
        
        return jsonify({
            'data': data,
            'view_type': view_type,
            'count': len(data)
        })
    
    except Exception as e:
        logger.error(f"Get map data error: {e}")
        return jsonify({'error': 'Failed to get map data'}), 500

# Helper Functions
def save_project_to_db(result):
    """Save processed project to database"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        project = result['project_overview']
        resources = result['resource_estimate']
        location = result['location']
        risk = result['risk_assessment']
        investment = result['investment_metrics']
        
        cursor.execute('''
            INSERT INTO projects (
                project_name, company, state, county, commodities, report_date, report_type,
                coordinates_lat, coordinates_lon, township, section,
                resource_indicated_tonnage, resource_indicated_grade, resource_indicated_ounces,
                resource_inferred_tonnage, resource_inferred_grade, resource_inferred_ounces,
                risk_overall, risk_technical, risk_regulatory, risk_market, risk_infrastructure,
                estimated_value, attractiveness_score, roi, payback_period, pdf_filename
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            project.get('project_name'),
            project.get('company'),
            project.get('state'),
            project.get('county'),
            ','.join(project.get('commodities', [])),
            project.get('report_date'),
            project.get('report_type'),
            location['coordinates'].get('lat'),
            location['coordinates'].get('lon'),
            location.get('township'),
            location.get('section'),
            resources['indicated'].get('tonnage'),
            resources['indicated'].get('grade'),
            resources['indicated'].get('ounces'),
            resources['inferred'].get('tonnage'),
            resources['inferred'].get('grade'),
            resources['inferred'].get('ounces'),
            risk.get('overall'),
            risk.get('technical'),
            risk.get('regulatory'),
            risk.get('market'),
            risk.get('infrastructure'),
            investment.get('estimated_value'),
            investment.get('attractiveness_score'),
            investment.get('roi'),
            investment.get('payback_period'),
            result.get('filename')
        ))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database save error: {e}")

def background_scraping(states, sources):
    """Background scraping process"""
    try:
        scraper = WebScraper()
        
        if 'sedar' in sources:
            for result in scraper.scrape_sedar_plus(states, max_results=10):
                save_scraping_result_to_db(result)
        
        if 'companies' in sources:
            for result in scraper.scrape_company_websites(SAMPLE_COMPANIES, max_results=5):
                save_scraping_result_to_db(result)
                
    except Exception as e:
        logger.error(f"Background scraping error: {e}")

def save_scraping_result_to_db(result):
    """Save scraping result to database"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO scraping_results (
                project_name, company, state, commodities, resource_grade, tonnage,
                coordinates_lat, coordinates_lon, risk_score, source_url, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            result.get('project_name'),
            result.get('company'),
            result.get('state'),
            ','.join(result.get('commodities', [])),
            result.get('resource_grade'),
            result.get('tonnage'),
            result.get('coordinates_lat'),
            result.get('coordinates_lon'),
            result.get('risk_score'),
            result.get('source_url'),
            result.get('last_updated')
        ))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Scraping result save error: {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

