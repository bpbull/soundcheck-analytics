"""
Soundcheck Analytics - Data Generation Script
Generates realistic fake data for a live music analytics platform
Author: Brendan Bullivant
Date: 2025

This script creates interconnected data with realistic patterns including:
- Seasonal trends (more summer events)
- Day-of-week patterns (Thursday shows rate higher)
- Geographic patterns (music cities have more active users)
- Data quality issues (e.g. 15% duplicate ratings for testing)
"""

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Tuple
import hashlib

# Set seeds for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

# ================================================================================
# CONSTANTS AND CONFIGURATION
# ================================================================================

# Genre relationships and preferences
GENRES = {
    'primary': [
        'rock', 'pop', 'hip-hop', 'country', 'electronic', 'indie', 
        'jazz', 'classical', 'metal', 'punk', 'folk', 'r&b', 'reggae'
    ],
    'related': {
        'rock': ['alternative', 'indie', 'punk', 'metal', 'grunge'],
        'pop': ['dance', 'indie-pop', 'synth-pop', 'electropop'],
        'hip-hop': ['rap', 'trap', 'underground', 'conscious'],
        'country': ['folk', 'americana', 'bluegrass', 'outlaw'],
        'electronic': ['house', 'techno', 'dubstep', 'ambient', 'trance'],
        'indie': ['indie-rock', 'indie-folk', 'dream-pop', 'lo-fi'],
        'jazz': ['bebop', 'fusion', 'smooth', 'free'],
        'metal': ['heavy', 'death', 'black', 'progressive'],
    }
}

# City data with music scene characteristics
CITIES_DATA = [
    {'city': 'Austin', 'state': 'TX', 'population': 978908, 'scene_score': 9.5, 'genres': ['rock', 'country', 'indie'], 'avg_ticket': 48},
    {'city': 'Nashville', 'state': 'TN', 'population': 689447, 'scene_score': 9.8, 'genres': ['country', 'rock', 'folk'], 'avg_ticket': 45},
    {'city': 'Los Angeles', 'state': 'CA', 'population': 3967000, 'scene_score': 9.0, 'genres': ['pop', 'hip-hop', 'electronic'], 'avg_ticket': 65},
    {'city': 'New York', 'state': 'NY', 'population': 8336000, 'scene_score': 9.7, 'genres': ['jazz', 'hip-hop', 'indie', 'punk'], 'avg_ticket': 75},
    {'city': 'Seattle', 'state': 'WA', 'population': 737015, 'scene_score': 8.8, 'genres': ['grunge', 'indie', 'electronic'], 'avg_ticket': 55},
    {'city': 'Portland', 'state': 'OR', 'population': 652503, 'scene_score': 8.5, 'genres': ['indie', 'folk', 'punk'], 'avg_ticket': 45},
    {'city': 'Chicago', 'state': 'IL', 'population': 2747000, 'scene_score': 8.9, 'genres': ['blues', 'jazz', 'hip-hop', 'house'], 'avg_ticket': 50},
    {'city': 'San Francisco', 'state': 'CA', 'population': 874784, 'scene_score': 8.6, 'genres': ['electronic', 'indie', 'jazz'], 'avg_ticket': 70},
    {'city': 'Denver', 'state': 'CO', 'population': 715522, 'scene_score': 8.3, 'genres': ['jam', 'folk', 'electronic'], 'avg_ticket': 50},
    {'city': 'Atlanta', 'state': 'GA', 'population': 498000, 'scene_score': 8.7, 'genres': ['hip-hop', 'r&b', 'trap'], 'avg_ticket': 55},
    {'city': 'Miami', 'state': 'FL', 'population': 442241, 'scene_score': 8.4, 'genres': ['electronic', 'latin', 'hip-hop'], 'avg_ticket': 60},
    {'city': 'Boston', 'state': 'MA', 'population': 694583, 'scene_score': 8.2, 'genres': ['punk', 'indie', 'folk'], 'avg_ticket': 55},
    {'city': 'Philadelphia', 'state': 'PA', 'population': 1584000, 'scene_score': 8.3, 'genres': ['hip-hop', 'punk', 'indie'], 'avg_ticket': 48},
    {'city': 'Detroit', 'state': 'MI', 'population': 674841, 'scene_score': 8.5, 'genres': ['techno', 'rock', 'hip-hop'], 'avg_ticket': 40},
    {'city': 'Minneapolis', 'state': 'MN', 'population': 425403, 'scene_score': 8.1, 'genres': ['indie', 'hip-hop', 'folk'], 'avg_ticket': 45},
]

# Venue name components for generation
VENUE_PREFIXES = ['The', 'Club', 'Bar', '']
VENUE_NAMES = [
    'Fillmore', 'Roxy', 'Troubadour', 'Blue Note', 'Bowery', 'Apollo', 'Whisky',
    'Viper Room', 'Mercury', 'Crystal', 'Paradise', 'Electric', 'Satellite',
    'Underground', 'Basement', 'Rooftop', 'Garden', 'Hall', 'Lounge', 'Stage'
]
VENUE_SUFFIXES = ['Room', 'Ballroom', 'Theater', 'Arena', 'Amphitheater', 'Club', 'Tavern', 'House']

# Artist name patterns
ARTIST_PATTERNS = [
    'The {adjective} {nouns}',
    '{first_name} and the {nouns}',
    '{adjective} {noun}',
    'The {nouns}',
    '{first_name} {last_name}',
    '{word} {word}',
]

# ================================================================================
# DATA GENERATOR CLASS
# ================================================================================

class SoundcheckDataGenerator:
    """Main class for generating all Soundcheck platform data"""
    
    def __init__(self, output_dir='data/raw'):
        """
        Initialize the data generator
        
        Args:
            output_dir: Directory where CSV files will be saved
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Data containers
        self.cities = []
        self.users = []
        self.artists = []
        self.venues = []
        self.tours = []
        self.events = []
        self.event_ratings = []
        self.venue_reviews = []
        self.artist_ratings = []
        self.ticket_sales = []
        self.user_follows = []
        
        # Tracking for relationships
        self.user_rating_counts = {}  # Track how many ratings each user has made
        self.venue_event_counts = {}  # Track events per venue
        
    def generate_all_data(self):
        """Main method to generate all data in correct order"""
        print("Starting Soundcheck Data Generation...")
        
        print("Generating cities...")
        self.generate_cities()
        
        print("Generating users...")
        self.generate_users(n=10000)
        
        print("Generating artists...")
        self.generate_artists(n=2000)
        
        print("Generating venues...")
        self.generate_venues(n=500)
        
        print("Generating tours...")
        self.generate_tours(n=500)
        
        print("Generating events...")
        self.generate_events(n=10000)
        
        print("Generating event ratings...")
        self.generate_event_ratings()
        
        print("Generating venue reviews...")
        self.generate_venue_reviews()
        
        print("Generating artist ratings...")
        self.generate_artist_ratings()
        
        print("Generating ticket sales...")
        self.generate_ticket_sales()
        
        print("Generating user follows...")
        self.generate_user_follows()
        
        print("Saving all data to CSV...")
        self.save_all_to_csv()
        
        print("Data generation complete!")
        self.print_summary_statistics()
    
    # ============================================================================
    # CITIES GENERATION
    # ============================================================================
    
    def generate_cities(self):
        """Generate city lookup data"""
        for idx, city_data in enumerate(CITIES_DATA):
            city = {
                'city_id': f'CITY_{idx+1:03d}',
                'city': city_data['city'],
                'state': city_data['state'],
                'population': city_data['population'],
                'music_scene_score': city_data['scene_score'],
                'primary_genres': json.dumps(city_data['genres']),
                'avg_ticket_price': city_data['avg_ticket'],
                'total_venues': random.randint(20, 200),
                'timezone': self._get_timezone(city_data['state'])
            }
            self.cities.append(city)
    
    def _get_timezone(self, state):
        """Map state to timezone"""
        timezone_map = {
            'CA': 'America/Los_Angeles', 'WA': 'America/Los_Angeles', 'OR': 'America/Los_Angeles',
            'TX': 'America/Chicago', 'IL': 'America/Chicago', 'TN': 'America/Chicago',
            'NY': 'America/New_York', 'MA': 'America/New_York', 'PA': 'America/New_York',
            'GA': 'America/New_York', 'FL': 'America/New_York', 'MI': 'America/Detroit',
            'CO': 'America/Denver', 'MN': 'America/Chicago'
        }
        return timezone_map.get(state, 'America/New_York')
    
    # ============================================================================
    # USERS GENERATION
    # ============================================================================
    
    def generate_users(self, n=10000):
        """Generate user data with realistic distributions"""
        
        # User type distribution
        power_user_count = int(n * 0.1)  # 10% power users
        regular_user_count = int(n * 0.3)  # 30% regular users
        casual_user_count = n - power_user_count - regular_user_count  # 60% casual
        
        user_id_counter = 1
        
        # Generate different user types
        for user_type, count in [
            ('power_user', power_user_count),
            ('regular', regular_user_count),
            ('casual', casual_user_count)
        ]:
            for _ in range(count):
                city = random.choice(self.cities)
                
                # Users from high scene score cities are more likely to be verified
                verified_chance = 0.5 if city['music_scene_score'] > 9 else 0.2
                
                # Age distribution (music fans skew younger)
                age_weights = {
                    '18-24': 0.30,
                    '25-34': 0.35,
                    '35-44': 0.20,
                    '45-54': 0.10,
                    '55+': 0.05
                }
                age_group = random.choices(
                    list(age_weights.keys()),
                    weights=list(age_weights.values())
                )[0]
                
                # Genre preferences based on city and age
                city_genres = json.loads(city['primary_genres'])
                num_genres = 3 if user_type == 'power_user' else random.randint(1, 3)
                preferred_genres = self._get_user_genre_preferences(city_genres, age_group, num_genres)
                
                # Join date (power users tend to be early adopters)
                if user_type == 'power_user':
                    join_date = fake.date_between(start_date='-5y', end_date='-2y')
                elif user_type == 'regular':
                    join_date = fake.date_between(start_date='-3y', end_date='-6m')
                else:
                    join_date = fake.date_between(start_date='-2y', end_date='today')
                
                user = {
                    'user_id': f'USR_{user_id_counter:05d}',
                    'username': self._generate_username(),
                    'email': fake.email(),
                    'user_type': 'verified' if random.random() < verified_chance else 'regular',
                    'user_segment': user_type,
                    'join_date': join_date,
                    'home_city': city['city'],
                    'home_state': city['state'],
                    'age_group': age_group,
                    'preferred_genres': json.dumps(preferred_genres),
                    'profile_completeness': random.choice([0.25, 0.5, 0.75, 1.0]),
                    'email_verified': random.random() < 0.7,
                    'push_notifications_enabled': random.random() < 0.4,
                    'last_active_date': fake.date_between(start_date=join_date, end_date='today')
                }
                
                self.users.append(user)
                self.user_rating_counts[user['user_id']] = 0
                user_id_counter += 1
    
    def _generate_username(self):
        """Generate realistic usernames"""
        patterns = [
            lambda: fake.user_name(),
            lambda: f"{fake.first_name().lower()}{random.randint(1, 999)}",
            lambda: f"music_{fake.word()}_{random.randint(1, 99)}",
            lambda: f"{random.choice(['concert', 'live', 'music', 'show'])}_{fake.word()}{random.randint(1, 999)}",
        ]
        return random.choice(patterns)()
    
    def _get_user_genre_preferences(self, city_genres, age_group, num_genres):
        """Generate genre preferences based on city and age"""
        # Age-based genre preferences
        age_genre_bias = {
            '18-24': ['pop', 'hip-hop', 'electronic', 'indie'],
            '25-34': ['indie', 'rock', 'electronic', 'hip-hop'],
            '35-44': ['rock', 'alternative', 'indie', 'country'],
            '45-54': ['rock', 'classic rock', 'country', 'jazz'],
            '55+': ['classic rock', 'jazz', 'classical', 'folk']
        }
        
        # Combine city and age preferences
        potential_genres = list(set(city_genres + age_genre_bias.get(age_group, [])))
        
        # Add some random genres for diversity
        all_genres = GENRES['primary']
        potential_genres.extend(random.sample(all_genres, min(2, len(all_genres))))
        
        # Remove duplicates and select
        potential_genres = list(set(potential_genres))
        return random.sample(potential_genres, min(num_genres, len(potential_genres)))
    
    # ============================================================================
    # ARTISTS GENERATION
    # ============================================================================
    
    def generate_artists(self, n=2000):
        """Generate artist data with realistic popularity distribution"""
        
        # Aff artist name variation for data quality issue
        self.artist_name_variations = {}

        for i in range(n):
            # Popularity follows power law distribution (few very popular, many unknown)
            popularity_tier = random.choices(
                ['megastar', 'popular', 'rising', 'established', 'emerging', 'local'],
                weights=[0.01, 0.04, 0.10, 0.20, 0.35, 0.30]
            )[0]
            
            # Set metrics based on tier
            tier_metrics = {
                'megastar': {
                    'spotify_listeners': (5000000, 50000000),
                    'instagram_followers': (1000000, 10000000),
                    'booking_price': (100000, 1000000)
                },
                'popular': {
                    'spotify_listeners': (1000000, 5000000),
                    'instagram_followers': (100000, 1000000),
                    'booking_price': (50000, 100000)
                },
                'rising': {
                    'spotify_listeners': (100000, 1000000),
                    'instagram_followers': (10000, 100000),
                    'booking_price': (10000, 50000)
                },
                'established': {
                    'spotify_listeners': (50000, 100000),
                    'instagram_followers': (5000, 10000),
                    'booking_price': (5000, 10000)
                },
                'emerging': {
                    'spotify_listeners': (10000, 50000),
                    'instagram_followers': (1000, 5000),
                    'booking_price': (1000, 5000)
                },
                'local': {
                    'spotify_listeners': (100, 10000),
                    'instagram_followers': (100, 1000),
                    'booking_price': (500, 1000)
                }
            }
            
            metrics = tier_metrics[popularity_tier]
            
            # Generate artist details
            primary_genre = random.choice(GENRES['primary'])
            origin_city = random.choice(self.cities)
            
            artist = {
                'artist_id': f'ART_{i+1:04d}',
                'artist_name': self._generate_artist_name(),
                'formed_year': random.randint(1970, 2024),
                'origin_city': origin_city['city'],
                'origin_state': origin_city['state'],
                'origin_country': 'USA',
                'spotify_monthly_listeners': random.randint(*metrics['spotify_listeners']),
                'instagram_followers': random.randint(*metrics['instagram_followers']),
                'genre_primary': primary_genre,
                'genre_secondary': random.choice(GENRES['related'].get(primary_genre, [primary_genre])),
                'booking_price_min': metrics['booking_price'][0],
                'booking_price_max': metrics['booking_price'][1],
                'popularity_tier': popularity_tier,
                'tour_frequency': random.choice(['rare', 'occasional', 'moderate', 'frequent', 'constant']),
                'average_show_duration_minutes': random.randint(45, 180),
                'has_label': popularity_tier in ['megastar', 'popular', 'rising'],
                'verified_artist': popularity_tier in ['megastar', 'popular']
            }
            
            # Create name variations for 5% of artists
            if random.random() < 0.05:
                base_name = artist['artist_name']
                variations = [
                    base_name.upper(),
                    base_name.lower(),
                    base_name.replace('The ', ''),  # Missing "The"
                    base_name + ' Band',
                    base_name.replace('and', '&'),
                ]
                self.artist_name_variations[artist['artist_id']] = variations

            self.artists.append(artist)
    
    def _generate_artist_name(self):
        """Generate creative artist names"""
        adjectives = [
            'Electric', 'Cosmic', 'Velvet', 'Crimson', 'Silver', 'Golden', 'Midnight',
            'Neon', 'Crystal', 'Shadow', 'Wild', 'Silent', 'Broken', 'Lost', 'Flying'
        ]
        nouns = [
            'Wolves', 'Tigers', 'Eagles', 'Ghosts', 'Dreams', 'Waves', 'Stars',
            'Lights', 'Shadows', 'Hearts', 'Souls', 'Minds', 'Riders', 'Drifters'
        ]
        
        patterns = [
            lambda: f"The {random.choice(adjectives)} {random.choice(nouns)}",
            lambda: f"{fake.first_name()} and the {random.choice(nouns)}",
            lambda: f"{random.choice(adjectives)} {random.choice(nouns)[:-1]}",
            lambda: f"The {random.choice(nouns)}",
            lambda: f"{fake.first_name()} {fake.last_name()}",
            lambda: f"{fake.last_name()}",
            lambda: f"{random.choice(adjectives).lower()}{random.choice(nouns).lower()}",
        ]
        
        return random.choice(patterns)()
    
    # ============================================================================
    # VENUES GENERATION  
    # ============================================================================
    
    def generate_venues(self, n=500):
        """Generate venue data with realistic capacity distributions"""
        
        venue_id_counter = 1
        
        for city in self.cities:
            # Number of venues proportional to city population and scene score
            num_venues = max(1, int(n * (city['population'] / 25000000) * (city['music_scene_score'] / 10)))
            
            for _ in range(min(num_venues, n - venue_id_counter + 1)):
                # Venue type distribution
                venue_type = random.choices(
                    ['club', 'bar', 'theater', 'arena', 'stadium', 'amphitheater', 'festival_grounds'],
                    weights=[0.30, 0.25, 0.20, 0.10, 0.05, 0.05, 0.05]
                )[0]
                
                # Capacity based on venue type
                capacity_ranges = {
                    'club': (100, 500),
                    'bar': (50, 200),
                    'theater': (500, 3000),
                    'arena': (5000, 20000),
                    'stadium': (20000, 80000),
                    'amphitheater': (2000, 15000),
                    'festival_grounds': (5000, 100000)
                }
                
                capacity = random.randint(*capacity_ranges[venue_type])
                
                # Standing room adds 20-50% for some venue types
                if venue_type in ['club', 'bar', 'theater']:
                    standing_room_capacity = capacity + int(capacity * random.uniform(0.2, 0.5))
                else:
                    standing_room_capacity = capacity
                
                # Historical venues opened earlier
                if random.random() < 0.2:  # 20% are historical
                    opened_year = random.randint(1850, 1970)
                else:
                    opened_year = random.randint(1970, 2023)
                
                # Generate venue name first
                venue_name = self._generate_venue_name(venue_type)
                # Clean it for URL
                clean_name = venue_name.lower().replace(' ', '').replace("'", '')
                
                venue = {
                    'venue_id': f'VEN_{venue_id_counter:04d}',
                    'venue_name': venue_name,
                    'address': fake.street_address(),
                    'city': city['city'],
                    'state': city['state'],
                    'zip_code': fake.postcode(),
                    'latitude': float(fake.latitude()),
                    'longitude': float(fake.longitude()),
                    'venue_type': venue_type,
                    'capacity': capacity,
                    'standing_room_capacity': standing_room_capacity,
                    'opened_year': opened_year,
                    'parking_available': random.random() < 0.6,
                    'valet_parking': venue_type in ['theater', 'arena'] and random.random() < 0.3,
                    'food_available': random.random() < 0.7,
                    'full_bar': venue_type != 'festival_grounds',
                    'accessible_ada': random.random() < 0.85,
                    'box_office': venue_type in ['theater', 'arena', 'stadium', 'amphitheater'],
                    'typical_ticket_fee': round(random.uniform(5, 25), 2),
                    'venue_website': f"www.{clean_name}.com" if random.random() < 0.7 else None,
                    'phone': fake.phone_number(),
                    'validated_capacity': random.random() < 0.8
                }
                
                self.venues.append(venue)
                self.venue_event_counts[venue['venue_id']] = 0
                venue_id_counter += 1
                
                if venue_id_counter > n:
                    break

    
    def _generate_venue_name(self, venue_type):
        """Generate venue names based on type"""
        if venue_type == 'club':
            patterns = [
                lambda: f"The {random.choice(['Underground', 'Basement', 'Loft', 'Cave', 'Den'])}",
                lambda: f"{random.choice(['Club', 'Night'])} {fake.last_name()}",
                lambda: f"The {fake.word().title()} Room"
            ]
        elif venue_type == 'bar':
            patterns = [
                lambda: f"{fake.last_name()}'s {random.choice(['Bar', 'Pub', 'Tavern', 'Taproom'])}",
                lambda: f"The {random.choice(['Crooked', 'Broken', 'Golden', 'Silver'])} {random.choice(['Crow', 'Fox', 'Lion', 'Eagle'])}",
            ]
        elif venue_type == 'theater':
            patterns = [
                lambda: f"The {fake.last_name()} Theater",
                lambda: f"{random.choice(['Paramount', 'Palace', 'Royal', 'Grand'])} Theater",
                lambda: f"The {fake.word().title()} Playhouse"
            ]
        elif venue_type in ['arena', 'stadium']:
            patterns = [
                lambda: f"{fake.company()} {venue_type.title()}",
                lambda: f"{fake.city()} {venue_type.title()}",
            ]
        else:
            patterns = [
                lambda: f"{fake.word().title()} {venue_type.replace('_', ' ').title()}",
            ]
        
        return random.choice(patterns)()
    
    # ============================================================================
    # TOURS GENERATION
    # ============================================================================
    
    def generate_tours(self, n=100):
        """Generate tour data"""
        
        # Only established artists go on tour
        touring_artists = [a for a in self.artists if a['popularity_tier'] in ['megastar', 'popular', 'rising', 'established']]

        # Only ~50% of eligible artists are on tour at any given time
        artists_actually_touring = random.sample(
        touring_artists, 
        min(n, int(len(touring_artists) * 0.5))
        )
        
        for i in range(min(n, len(touring_artists))):
            artist = touring_artists[i % len(touring_artists)]
            
            # Tour length based on artist tier
            tour_lengths = {
                'megastar': (20, 75),
                'popular': (15, 35),
                'rising': (10, 25),
                'established': (5, 25)
            }
            
            num_shows = random.randint(*tour_lengths.get(artist['popularity_tier'], (5, 15)))
            
            # Tour timing (summer and fall are popular)
            start_month = random.choices(
                list(range(1, 13)),
                weights=[1, 1, 2, 3, 4, 5, 5, 4, 3, 2, 1, 1]
            )[0]
            
            start_date = datetime(2023 + random.randint(0, 2), start_month, random.randint(1, 28))
            # Tours last 2-4 months typically
            end_date = start_date + timedelta(days=random.randint(60, 120))
            
            tour_names = [
                f"{artist['artist_name']} World Tour {start_date.year}",
                f"The {fake.word().title()} Tour",
                f"{artist['artist_name']} - {fake.word().title()} {fake.word().title()} Tour",
                f"{random.choice(['Summer', 'Fall', 'Spring', 'Winter'])} Tour {start_date.year}",
            ]
            
            tour = {
                'tour_id': f'TOUR_{i+1:03d}',
                'tour_name': random.choice(tour_names),
                'artist_id': artist['artist_id'],
                'start_date': start_date.date(),
                'end_date': end_date.date(),
                'number_of_shows': num_shows,
                'tour_type': random.choice(['headlining', 'co-headlining', 'supporting', 'festival']),
                'tour_legs': random.randint(1, 3),  # Tours often have multiple legs
                'production_level': artist['popularity_tier'],  # Bigger artists = bigger production
            }
            
            self.tours.append(tour)
    
    # ============================================================================
    # EVENTS GENERATION
    # ============================================================================
    
    def generate_events(self, n=10000):
        """Generate event data with realistic patterns"""
        
        event_id_counter = 1
        
        # Create date range for events
        start_date = datetime.now() - timedelta(days=365 * 2)  # 2 years back
        end_date = datetime.now() + timedelta(days=365)  # 1 year forward
        
        # Track which artists are busy on which dates (no double booking)
        artist_calendar = {artist['artist_id']: [] for artist in self.artists}
        
        # Generate tour events first
        for tour in self.tours:
            tour_event_count = 0
            current_date = datetime.strptime(str(tour['start_date']), '%Y-%m-%d')
            tour_end = datetime.strptime(str(tour['end_date']), '%Y-%m-%d')
            
            while tour_event_count < tour['number_of_shows'] and current_date <= tour_end and event_id_counter <= n:
                # Tours typically have 2-4 days between shows
                if tour_event_count > 0:
                    current_date += timedelta(days=random.randint(2, 4))
                
                # Check if artist is available
                if current_date.date() not in artist_calendar[tour['artist_id']]:
                    venue = random.choice(self.venues)
                    
                    event = self._create_event(
                        event_id=f'EVT_{event_id_counter:05d}',
                        artist_id=tour['artist_id'],
                        venue=venue,
                        event_date=current_date,
                        tour_id=tour['tour_id']
                    )
                    
                    self.events.append(event)
                    artist_calendar[tour['artist_id']].append(current_date.date())
                    self.venue_event_counts[venue['venue_id']] += 1
                    event_id_counter += 1
                    tour_event_count += 1
        
        # Generate non-tour events
        while event_id_counter <= n:
            artist = random.choice(self.artists)
            venue = random.choice(self.venues)
            
            # Generate date with seasonal bias
            event_date = self._generate_event_date(start_date, end_date)
            
            # Check artist availability
            if event_date.date() not in artist_calendar[artist['artist_id']]:
                event = self._create_event(
                    event_id=f'EVT_{event_id_counter:05d}',
                    artist_id=artist['artist_id'],
                    venue=venue,
                    event_date=event_date,
                    tour_id=None
                )
                
                self.events.append(event)
                artist_calendar[artist['artist_id']].append(event_date.date())
                self.venue_event_counts[venue['venue_id']] += 1
                event_id_counter += 1
    
    def _create_event(self, event_id, artist_id, venue, event_date, tour_id=None):
        """Create a single event with all attributes"""
        
        artist = next(a for a in self.artists if a['artist_id'] == artist_id)

        if artist_id in self.artist_name_variations and random.random() < 0.1:
            artist_display_name = random.choice(self.artist_name_variations[artist_id])
        else:
            artist_display_name = artist['artist_name']
        
        # Pricing based on artist tier and venue
        base_prices = {
            'megastar': (75, 250),
            'popular': (50, 150),
            'rising': (35, 75),
            'established': (25, 60),
            'emerging': (15, 40),
            'local': (10, 25)
        }
        
        price_range = base_prices[artist['popularity_tier']]
        base_price = random.uniform(*price_range)
        
        # Venue type affects pricing
        if venue['venue_type'] == 'stadium':
            base_price *= 1.5
        elif venue['venue_type'] == 'arena':
            base_price *= 1.3
        elif venue['venue_type'] == 'club':
            base_price *= 0.8
        
        # Weekend premium
        if event_date.weekday() in [4, 5]:  # Friday/Saturday
            base_price *= 1.2
        
        # Popular artists announce earlier
        if artist['popularity_tier'] in ['megastar', 'popular']:
            announce_days = random.randint(90, 180)
        else:
            announce_days = random.randint(30, 90)
        
        announced_date = event_date - timedelta(days=announce_days)
        on_sale_date = announced_date + timedelta(days=random.randint(1, 7))
        
        # Generate opening acts for bigger shows
        opening_acts = []
        if venue['venue_type'] in ['arena', 'stadium', 'amphitheater']:
            num_openers = random.randint(1, 2)
            potential_openers = [a for a in self.artists if a['popularity_tier'] in ['emerging', 'local']]
            opening_acts = [a['artist_name'] for a in random.sample(potential_openers, min(num_openers, len(potential_openers)))]
        
        # Determine event status
        if event_date.date() < datetime.now().date():
            # Past events - most completed, some cancelled
            if random.random() < 0.05:  # 5% cancellation rate
                event_status = 'cancelled'
                cancellation_reason = random.choice([
                    'illness', 'weather', 'low ticket sales', 
                    'production issues', 'scheduling conflict'
                ])
            else:
                event_status = 'completed'
                cancellation_reason = None
        else:
            # Future events
            event_status = 'scheduled'
            cancellation_reason = None
        
        # Estimated attendance based on various factors
        if event_status == 'completed':
            # Popularity affects turnout
            if artist['popularity_tier'] == 'megastar':
                capacity_fill = random.uniform(0.85, 1.0)
            elif artist['popularity_tier'] == 'popular':
                capacity_fill = random.uniform(0.70, 0.95)
            else:
                capacity_fill = random.uniform(0.40, 0.85)
            
            # Thursday shows actually do better (true fans)
            if event_date.weekday() == 3:
                capacity_fill = min(1.0, capacity_fill * 1.1)
            
            estimated_attendance = int(venue['capacity'] * capacity_fill)
        else:
            estimated_attendance = None
        
        event = {
            'event_id': event_id,
            'event_name': f"{artist_display_name} at {venue['venue_name']}",
            'artist_id': artist_id,
            'venue_id': venue['venue_id'],
            'tour_id': tour_id,
            'event_date': event_date.date(),
            'event_day_of_week': event_date.strftime('%A'),
            'doors_time': self._generate_show_time('doors'),
            'show_time': self._generate_show_time('show'),
            'announced_date': announced_date.date(),
            'on_sale_date': on_sale_date.date(),
            'base_ticket_price': round(base_price, 2),
            'vip_ticket_price': round(base_price * 2.5, 2) if random.random() < 0.7 else None,
            'ticket_vendor': random.choice(['Ticketmaster', 'AXS', 'SeatGeek', 'Venue Box Office', 'Dice']),
            'age_restriction': random.choice(['All Ages', '18+', '21+', None]),
            'opening_acts': json.dumps(opening_acts) if opening_acts else None,
            'event_status': event_status,
            'cancellation_reason': cancellation_reason,
            'estimated_attendance': estimated_attendance,
            'weather_condition': self._get_weather(event_date) if venue['venue_type'] in ['amphitheater', 'festival_grounds'] else None,
            'special_event': random.random() < 0.05  # 5% are special (album release, holiday, etc.)
        }
        
        return event
    
    def _generate_event_date(self, start_date, end_date):
        """Generate event date with realistic patterns"""
        # More events on weekends
        day_weights = {
            0: 0.08,  # Monday
            1: 0.08,  # Tuesday  
            2: 0.10,  # Wednesday
            3: 0.15,  # Thursday (good for true fans)
            4: 0.24,  # Friday
            5: 0.28,  # Saturday
            6: 0.07   # Sunday
        }
        
        # Generate random date
        days_range = (end_date - start_date).days
        random_days = random.randint(0, days_range)
        potential_date = start_date + timedelta(days=random_days)
        
        # Adjust to preferred day of week
        target_dow = random.choices(list(day_weights.keys()), weights=list(day_weights.values()))[0]
        days_until_target = (target_dow - potential_date.weekday()) % 7
        
        return potential_date + timedelta(days=days_until_target)
    
    def _generate_show_time(self, time_type):
        """Generate realistic show times"""
        if time_type == 'doors':
            hour = random.choices([18, 19, 20], weights=[0.2, 0.6, 0.2])[0]
            minute = random.choice([0, 30])
        else:  # show time
            hour = random.choices([19, 20, 21], weights=[0.2, 0.6, 0.2])[0]
            minute = random.choice([0, 30])
        
        return f"{hour:02d}:{minute:02d}:00"
    
    def _get_weather(self, event_date):
        """Generate weather based on season"""
        month = event_date.month
        
        if month in [12, 1, 2]:  # Winter
            return random.choice(['clear', 'cold', 'snow', 'rain'])
        elif month in [3, 4, 5]:  # Spring
            return random.choice(['clear', 'partly cloudy', 'rain', 'mild'])
        elif month in [6, 7, 8]:  # Summer
            return random.choice(['clear', 'hot', 'partly cloudy', 'thunderstorm'])
        else:  # Fall
            return random.choice(['clear', 'cool', 'partly cloudy', 'rain'])
    
    # ============================================================================
    # RATINGS GENERATION
    # ============================================================================
    
    def generate_event_ratings(self):
        """Generate event ratings with realistic patterns and biases"""
        
        # Create lookup dictionaries for O(1) access instead of O(n)
        artists_dict = {a['artist_id']: a for a in self.artists}
        venues_dict = {v['venue_id']: v for v in self.venues}
        
        rating_id_counter = 1
        completed_events = [e for e in self.events if e['event_status'] == 'completed']
        
        print(f"  Generating ratings for {len(completed_events):,} completed events...")
        
        # Process events with progress updates
        for i, event in enumerate(completed_events):
            if i % 1000 == 0:
                print(f"    Processed {i:,}/{len(completed_events):,} events ({i/len(completed_events)*100:.1f}%)")
            
            # Now these are instant lookups
            artist = artists_dict[event['artist_id']]
            venue = venues_dict[event['venue_id']]
            
            # Calculate expected number of ratings
            base_ratings = 10
            
            # Artist popularity multiplier
            popularity_multipliers = {
                'megastar': 10,
                'popular': 5,
                'rising': 3,
                'established': 2,
                'emerging': 1.5,
                'local': 1
            }
            base_ratings *= popularity_multipliers[artist['popularity_tier']]
            
            # Venue size factor
            if venue['capacity'] > 10000:
                base_ratings *= 2
            elif venue['capacity'] > 1000:
                base_ratings *= 1.5
            
            # Add randomness
            num_ratings = max(1, int(random.gauss(base_ratings, base_ratings * 0.3)))
            
            # Determine base rating score for this event
            base_score = self._calculate_base_rating_score(event, artist, venue)
            
            # Generate individual ratings
            for _ in range(num_ratings):
                # Select user (faster random selection)
                user = self.users[random.randint(0, len(self.users) - 1)]
                
                # Generate rating with user bias
                rating_score = self._generate_rating_score(base_score, user, event)
                
                if random.random() < 0.02:  # 2% chance of time zone issue
                    # Rating appears BEFORE the event (timezone confusion)
                    days_before = random.randint(1, 3)
                    rating_date = datetime.strptime(str(event['event_date']), '%Y-%m-%d') - timedelta(days=days_before)
                    days_after = -days_before  # Negative days_after
                else:
                    # Normal rating timing
                    days_after = self._generate_days_after_event()
                    rating_date = datetime.strptime(str(event['event_date']), '%Y-%m-%d') + timedelta(days=days_after)
             
                # Generate review text for some ratings
                has_review = random.random() < 0.3  # 30% write reviews
                if has_review:
                    review_title, review_text = self._generate_review_text(rating_score)
                else:
                    review_title, review_text = None, None
                
                rating = {
                    'rating_id': f'RAT_{rating_id_counter:06d}',
                    'event_id': event['event_id'],
                    'user_id': user['user_id'],
                    'rating_score': rating_score,
                    'rating_date': rating_date.date(),
                    'days_after_event': days_after,
                    'review_title': review_title,
                    'review_text': review_text,
                    'verified_attendance': random.random() < 0.7,  # 70% verified
                    'helpful_count': random.randint(0, 20) if has_review else 0, 
                    'reported': random.random() < 0.01,  # 1% get reported
                    'aspects': self._generate_aspect_ratings(rating_score, venue, artist)
                }
                
                self.event_ratings.append(rating)
                self.user_rating_counts[user['user_id']] += 1
                rating_id_counter += 1
        
        print(f"    Generated {len(self.event_ratings):,} ratings")
        
        # Add duplicate ratings and bot attacks (data quality issue for pipeline to handle)
        self._add_duplicate_ratings()
        self._add_bot_attacks()

    
    def _calculate_base_rating_score(self, event, artist, venue):
        """Calculate base rating for an event based on various factors"""
        
        base_score = 4.0  # Start neutral
        
        # Day of week effect (Thursday shows rate higher - true fans)
        if event['event_day_of_week'] == 'Thursday':
            base_score += 0.3
        elif event['event_day_of_week'] == 'Saturday':
            base_score -= 0.1  # Crowded, casual crowd
        
        # Venue type effect
        venue_adjustments = {
            'club': 0.2,  # Intimate
            'theater': 0.3,  # Good acoustics
            'bar': 0.1,
            'arena': -0.1,  # Less intimate
            'stadium': -0.3,  # Poor sound, far from stage
            'amphitheater': 0.1,
            'festival_grounds': -0.1
        }
        base_score += venue_adjustments.get(venue['venue_type'], 0)
        
        # Weather effect for outdoor venues
        if event['weather_condition']:
            weather_adjustments = {
                'clear': 0.2,
                'mild': 0.1,
                'partly cloudy': 0,
                'rain': -0.4,
                'snow': -0.5,
                'thunderstorm': -0.6,
                'hot': -0.2,
                'cold': -0.3
            }
            base_score += weather_adjustments.get(event['weather_condition'], 0)
        
        # Special events rate higher
        if event.get('special_event'):
            base_score += 0.4
        
        # Add randomness for variety
        base_score += random.gauss(0, 0.2)
        
        return max(1.0, min(5.0, base_score))
    
    def _select_rating_user(self):
        """Select a user to make a rating (power users rate more)"""
        # Weight selection by user type
        user_weights = []
        for user in self.users:
            if user['user_segment'] == 'power_user':
                weight = 10
            elif user['user_segment'] == 'regular':
                weight = 3
            else:
                weight = 1
            user_weights.append(weight)
        
        return random.choices(self.users, weights=user_weights)[0]
    
    def _generate_rating_score(self, base_score, user, event):
        """Generate individual rating based on base score and user characteristics"""
        
        score = base_score
        
        # User type affects rating behavior
        if user['user_segment'] == 'power_user':
            # Power users are more critical
            score -= 0.2
            # But less variance (they know what to expect)
            score += random.gauss(0, 0.3)
        elif user['user_segment'] == 'casual':
            # Casual users rate higher
            score += 0.1
            # More variance
            score += random.gauss(0, 0.5)
        else:
            score += random.gauss(0, 0.4)
        
        # Verified users slightly more critical
        if user['user_type'] == 'verified':
            score -= 0.1
        
        # Round to nearest 0.5
        score = round(score * 2) / 2
        
        return max(1.0, min(5.0, score))
    
    def _generate_days_after_event(self):
        """Most ratings come within first week"""
        # 60% within 3 days, 30% within week, 10% later
        rand = random.random()
        if rand < 0.6:
            return random.randint(1, 3)
        elif rand < 0.9:
            return random.randint(4, 7)
        else:
            return random.randint(8, 30)
    
    def _generate_review_text(self, rating_score):
        """Generate review title and text based on rating"""
        
        if rating_score >= 4.5:
            titles = [
                "Amazing show!", "Best concert ever!", "Incredible performance!",
                "Mind-blowing!", "Unforgettable night!", "Absolutely phenomenal!"
            ]
            texts = [
                "The energy was electric from start to finish. The band was on fire and the crowd was totally into it.",
                "Perfect setlist, amazing sound quality, and incredible stage presence. Couldn't ask for more!",
                "This is why live music matters. An absolutely transcendent experience.",
            ]
        elif rating_score >= 3.5:
            titles = [
                "Great show", "Really enjoyed it", "Solid performance",
                "Good night out", "Worth seeing", "Entertaining show"
            ]
            texts = [
                "Overall a good show with a few minor issues. The band played well and the venue was decent.",
                "Enjoyed the performance though the sound could have been better. Still recommend seeing them live.",
                "Good energy from the band, crowd was into it. Venue was a bit crowded but manageable.",
            ]
        elif rating_score >= 2.5:
            titles = [
                "Just okay", "Mixed feelings", "Could've been better",
                "Average show", "Some issues", "Meh"
            ]
            texts = [
                "The performance was okay but nothing special. Sound issues throughout the night.",
                "Band seemed tired, setlist was predictable. Venue was overcrowded.",
                "Expected more based on their recordings. Live performance was disappointing.",
            ]
        else:
            titles = [
                "Disappointing", "Not worth it", "Poor experience",
                "Skip this one", "Waste of money", "Terrible"
            ]
            texts = [
                "Major sound problems, could barely hear the vocals. Band seemed unprepared.",
                "Venue was a disaster - oversold, no air conditioning, terrible acoustics.",
                "Band showed up late, played for 45 minutes, no encore. Complete waste of time and money.",
            ]
        
        return random.choice(titles), random.choice(texts)
    
    def _generate_aspect_ratings(self, overall_score, venue, artist):
        """Generate detailed aspect ratings"""
        
        aspects = {}
        
        # Base aspects on overall score with some variance
        base = overall_score
        
        # Sound quality affected by venue type
        sound_adjustment = {
            'theater': 0.5,
            'club': 0.2,
            'arena': -0.3,
            'stadium': -0.5
        }
        aspects['sound_quality'] = round(min(5, max(1, 
            base + sound_adjustment.get(venue['venue_type'], 0) + random.gauss(0, 0.3))) * 2) / 2
        
        # Venue experience
        venue_score = base + random.gauss(0, 0.4)
        if venue['parking_available']:
            venue_score += 0.2
        if venue['accessible_ada']:
            venue_score += 0.1
        aspects['venue_experience'] = round(min(5, max(1, venue_score)) * 2) / 2
        
        # Performance energy (artists usually bring it)
        aspects['performance_energy'] = round(min(5, max(1, base + random.gauss(0.2, 0.3))) * 2) / 2
        
        # Setlist satisfaction
        aspects['setlist_satisfaction'] = round(min(5, max(1, base + random.gauss(0, 0.5))) * 2) / 2
        
        # Crowd vibe
        aspects['crowd_vibe'] = round(min(5, max(1, base + random.gauss(0, 0.4))) * 2) / 2
        
        # Value for money (inversely related to price perception)
        aspects['value_for_money'] = round(min(5, max(1, base - 0.5 + random.gauss(0, 0.5))) * 2) / 2
        
        return json.dumps(aspects)
    
    def _add_duplicate_ratings(self):
        """Add 15% duplicate ratings as a data quality issue"""
        num_duplicates = int(len(self.event_ratings) * 0.15)
        duplicates_to_add = random.sample(self.event_ratings, min(num_duplicates, len(self.event_ratings)))
        
        for dup in duplicates_to_add:
            new_dup = dup.copy()
            new_dup['rating_id'] = f"RAT_{len(self.event_ratings) + len(duplicates_to_add):06d}"
            # Keep same user and event to make it a true duplicate
            self.event_ratings.append(new_dup)
        
        print(f"  Added {len(duplicates_to_add)} duplicate ratings for data quality testing")

    def _add_bot_attacks(self):
        """Add suspicious bot rating patterns"""
        print(f"  Adding bot attack patterns...")
        
        # Select 1% of events to be bot-attacked
        attacked_events = random.sample(
            [e for e in self.events if e['event_status'] == 'completed'],
            k=int(len(self.events) * 0.01)
        )
        
        bot_ratings_added = 0
        rating_id_start = len(self.event_ratings) + 1
        
        for event in attacked_events:
            # Bot attacks happen within same hour
            attack_timestamp = datetime.strptime(str(event['event_date']), '%Y-%m-%d') + timedelta(days=1, hours=random.randint(0, 23))
            
            # Create 20-50 ratings in same hour
            num_bot_ratings = random.randint(20, 50)
            
            # Bot accounts (create fake users or use a subset)
            for i in range(num_bot_ratings):
                rating = {
                    'rating_id': f'RAT_{rating_id_start + bot_ratings_added:06d}',
                    'event_id': event['event_id'],
                    'user_id': f'USR_{random.randint(9000, 9999):05d}',  # Suspicious user ID range
                    'rating_score': 1.0 if random.random() < 0.8 else 5.0,  # Extreme ratings
                    'rating_date': (attack_timestamp + timedelta(minutes=random.randint(0, 59))).date(),
                    'days_after_event': 1,
                    'review_title': None,  # Bots don't write reviews
                    'review_text': None,
                    'verified_attendance': False,
                    'helpful_count': 0,
                    'reported': random.random() < 0.3,  # 30% get reported
                    'aspects': json.dumps({
                        'sound_quality': 1.0,
                        'venue_experience': 1.0,
                        'performance_energy': 1.0,
                        'setlist_satisfaction': 1.0,
                        'crowd_vibe': 1.0,
                        'value_for_money': 1.0
                    })
                }
                self.event_ratings.append(rating)
                bot_ratings_added += 1
        
        print(f"    Added {bot_ratings_added} bot ratings across {len(attacked_events)} events")
    
    # ============================================================================
    # VENUE REVIEWS GENERATION
    # ============================================================================
    
    def generate_venue_reviews(self):
        """Generate venue reviews separate from event ratings"""
        
        review_id_counter = 1
        
        for venue in self.venues:
            # Number of reviews based on venue popularity
            if venue['venue_type'] in ['arena', 'stadium']:
                num_reviews = random.randint(50, 200)
            elif venue['venue_type'] in ['theater', 'amphitheater']:
                num_reviews = random.randint(20, 100)
            else:
                num_reviews = random.randint(5, 50)
            
            for _ in range(num_reviews):
                user = random.choice(self.users)
                
                # Base rating on venue type and features
                base_rating = 3.5
                
                if venue['parking_available']:
                    base_rating += 0.2
                if venue['food_available']:
                    base_rating += 0.1
                if venue['accessible_ada']:
                    base_rating += 0.2
                if venue['venue_type'] == 'theater':
                    base_rating += 0.3
                elif venue['venue_type'] == 'stadium':
                    base_rating -= 0.3
                
                overall_rating = round(min(5, max(1, base_rating + random.gauss(0, 0.5))) * 2) / 2
                
                review = {
                    'review_id': f'VREV_{review_id_counter:05d}',
                    'venue_id': venue['venue_id'],
                    'user_id': user['user_id'],
                    'review_date': fake.date_between(start_date='-2y', end_date='today'),
                    'overall_rating': overall_rating,
                    'review_text': self._generate_venue_review_text(overall_rating, venue),
                    'aspects': self._generate_venue_aspects(overall_rating, venue)
                }
                
                self.venue_reviews.append(review)
                review_id_counter += 1
    
    def _generate_venue_review_text(self, rating, venue):
        """Generate venue review text"""
        
        if rating >= 4:
            texts = [
                f"Great venue with excellent sightlines. The {venue['venue_type']} has amazing acoustics.",
                "Easy to get to, plenty of parking, staff was super helpful. Will definitely come back!",
                "One of the best venues in the city. Sound quality is consistently excellent.",
            ]
        elif rating >= 3:
            texts = [
                "Decent venue but drinks are overpriced. Sound quality varies depending on where you stand.",
                "Good location but parking is a nightmare. Arrive early or take public transport.",
                "Nice venue but gets very crowded. Bathrooms could be cleaner.",
            ]
        else:
            texts = [
                "Poor acoustics, overcrowded, and overpriced everything. There are better venues in town.",
                "Terrible sightlines unless you're right up front. Drinks are ridiculously expensive.",
                "Avoid if possible. Bad sound, rude staff, and the whole place needs renovation.",
            ]
        
        return random.choice(texts)
    
    def _generate_venue_aspects(self, overall_rating, venue):
        """Generate venue aspect ratings"""
        
        aspects = {}
        base = overall_rating
        
        aspects['location_convenience'] = round(min(5, max(1, base + random.gauss(0, 0.5))) * 2) / 2
        aspects['sound_system'] = round(min(5, max(1, base + random.gauss(0, 0.4))) * 2) / 2
        aspects['sightlines'] = round(min(5, max(1, base + random.gauss(0, 0.4))) * 2) / 2
        aspects['cleanliness'] = round(min(5, max(1, base + random.gauss(-0.2, 0.3))) * 2) / 2
        aspects['staff_friendliness'] = round(min(5, max(1, base + random.gauss(0, 0.5))) * 2) / 2
        
        # Food and drink typically rate lower (overpriced)
        aspects['food_quality'] = round(min(5, max(1, base - 0.5 + random.gauss(0, 0.4))) * 2) / 2 if venue['food_available'] else None
        aspects['drink_prices'] = round(min(5, max(1, base - 1 + random.gauss(0, 0.5))) * 2) / 2
        
        # Parking
        if venue['parking_available']:
            aspects['parking'] = round(min(5, max(1, base + random.gauss(0, 0.5))) * 2) / 2
        else:
            aspects['parking'] = round(min(5, max(1, 2 + random.gauss(0, 0.5))) * 2) / 2
        
        aspects['bathroom_availability'] = round(min(5, max(1, base - 0.3 + random.gauss(0, 0.4))) * 2) / 2
        
        return json.dumps(aspects)
    
    # ============================================================================
    # ARTIST RATINGS GENERATION
    # ============================================================================
    
    def generate_artist_ratings(self):
        """Generate artist ratings"""
        
        rating_id_counter = 1
        
        for artist in self.artists:
            # Popular artists get more ratings
            if artist['popularity_tier'] == 'megastar':
                num_ratings = random.randint(500, 2000)
            elif artist['popularity_tier'] == 'popular':
                num_ratings = random.randint(100, 500)
            elif artist['popularity_tier'] == 'rising':
                num_ratings = random.randint(50, 200)
            else:
                num_ratings = random.randint(5, 50)
            
            # Artist quality based on tier
            tier_ratings = {
                'megastar': 4.3,
                'popular': 4.0,
                'rising': 3.8,
                'established': 3.6,
                'emerging': 3.4,
                'local': 3.2
            }
            
            base_rating = tier_ratings[artist['popularity_tier']]
            
            for _ in range(num_ratings):
                user = random.choice(self.users)
                
                overall_rating = round(min(5, max(1, base_rating + random.gauss(0, 0.5))) * 2) / 2
                
                rating = {
                    'artist_rating_id': f'ARAT_{rating_id_counter:05d}',
                    'artist_id': artist['artist_id'],
                    'user_id': user['user_id'],
                    'rating_date': fake.date_between(start_date='-2y', end_date='today'),
                    'overall_rating': overall_rating,
                    'aspects': self._generate_artist_aspects(overall_rating)
                }
                
                self.artist_ratings.append(rating)
                rating_id_counter += 1
    
    def _generate_artist_aspects(self, overall_rating):
        """Generate artist aspect ratings"""
        
        aspects = {}
        base = overall_rating
        
        aspects['live_performance'] = round(min(5, max(1, base + random.gauss(0.1, 0.3))) * 2) / 2
        aspects['stage_presence'] = round(min(5, max(1, base + random.gauss(0, 0.4))) * 2) / 2
        aspects['sound_quality'] = round(min(5, max(1, base + random.gauss(0, 0.3))) * 2) / 2
        aspects['fan_interaction'] = round(min(5, max(1, base + random.gauss(0, 0.5))) * 2) / 2
        aspects['setlist_variety'] = round(min(5, max(1, base + random.gauss(-0.2, 0.4))) * 2) / 2
        
        return json.dumps(aspects)
    
    # ============================================================================
    # TICKET SALES GENERATION
    # ============================================================================
    
    def generate_ticket_sales(self):
        """Generate simulated ticket sales data"""
        
        # Create lookup dictionaries for O(1) access
        artists_dict = {a['artist_id']: a for a in self.artists}
        venues_dict = {v['venue_id']: v for v in self.venues}
        
        sale_id_counter = 1
        completed_events = [e for e in self.events if e['estimated_attendance']]
        
        print(f"  Generating ticket sales for {len(completed_events):,} events...")
        
        for i, event in enumerate(completed_events):
            if i % 1000 == 0:
                print(f"    Processed {i:,}/{len(completed_events):,} events")
            
            artist = artists_dict[event['artist_id']]
            venue = venues_dict[event['venue_id']]
            
            # Calculate REALISTIC number of sales transactions
            avg_tickets_per_sale = 2.5
            num_sales = int(event['estimated_attendance'] / avg_tickets_per_sale)
            
            # Generate sales over time from on_sale_date to event_date
            on_sale = datetime.strptime(str(event['on_sale_date']), '%Y-%m-%d')
            event_date = datetime.strptime(str(event['event_date']), '%Y-%m-%d')
            
            for _ in range(num_sales):
                # Most sales happen early or close to event
                if random.random() < 0.4:  # 40% in first week
                    days_after_onsale = random.randint(0, 7)
                elif random.random() < 0.3:  # 30% in last week
                    days_before_event = random.randint(0, 7)
                    days_after_onsale = (event_date - on_sale).days - days_before_event
                else:  # 30% spread throughout
                    days_after_onsale = random.randint(8, max(8, (event_date - on_sale).days - 8))
                
                sale_date = on_sale + timedelta(days=max(0, days_after_onsale))
                
                # Ticket type based on availability
                if event['vip_ticket_price'] and random.random() < 0.15:
                    ticket_type = 'vip'
                    unit_price = event['vip_ticket_price']
                else:
                    ticket_type = 'general'
                    unit_price = event['base_ticket_price']
                
                quantity = random.choices([1, 2, 3, 4, 5, 6], weights=[0.2, 0.4, 0.15, 0.15, 0.05, 0.05])[0]
                fees = venue['typical_ticket_fee'] * quantity
                
                sale = {
                    'sale_id': f'TKT_{sale_id_counter:05d}',
                    'event_id': event['event_id'],
                    'sale_date': sale_date.date(),
                    'days_before_event': max(0, (event_date - sale_date).days),
                    'quantity_sold': quantity,
                    'ticket_type': ticket_type,
                    'unit_price': unit_price,
                    'fees': round(fees, 2),
                    'total_amount': round((unit_price * quantity) + fees, 2)
                }
                
                self.ticket_sales.append(sale)
                sale_id_counter += 1
        
        print(f"    Generated {len(self.ticket_sales):,} ticket sales")
    
    # ============================================================================
    # USER FOLLOWS GENERATION
    # ============================================================================
    
    def generate_user_follows(self):
        """Generate user-artist follow relationships"""
        
        follow_id_counter = 1
        
        for user in self.users:
            # Number of artists followed based on user type
            if user['user_segment'] == 'power_user':
                num_follows = random.randint(20, 100)
            elif user['user_segment'] == 'regular':
                num_follows = random.randint(5, 20)
            else:
                num_follows = random.randint(1, 5)
            
            # Get user's preferred genres
            preferred_genres = json.loads(user['preferred_genres'])
            
            # Select artists to follow (biased toward preferred genres)
            artists_to_follow = []
            
            # 70% from preferred genres
            genre_matched_artists = [
                a for a in self.artists 
                if a['genre_primary'] in preferred_genres or a['genre_secondary'] in preferred_genres
            ]
            num_genre_matches = min(int(num_follows * 0.7), len(genre_matched_artists))
            if num_genre_matches > 0:
                artists_to_follow.extend(random.sample(genre_matched_artists, num_genre_matches))
            
            # 30% random discovery
            remaining_follows = num_follows - len(artists_to_follow)
            if remaining_follows > 0:
                other_artists = [a for a in self.artists if a not in artists_to_follow]
                artists_to_follow.extend(random.sample(other_artists, min(remaining_follows, len(other_artists))))
            
            for artist in artists_to_follow:
                follow = {
                    'follow_id': f'FOL_{follow_id_counter:05d}',
                    'user_id': user['user_id'],
                    'artist_id': artist['artist_id'],
                    'follow_date': fake.date_between(start_date=user['join_date'], end_date='today'),
                    'notifications_enabled': random.random() < 0.3  # 30% enable notifications
                }
                
                self.user_follows.append(follow)
                follow_id_counter += 1
    
    # ============================================================================
    # DATA EXPORT FUNCTIONS
    # ============================================================================
    
    def save_all_to_csv(self):
        """Save all generated data to CSV files"""
        
        # Create dataframes and save
        datasets = {
            'cities': self.cities,
            'users': self.users,
            'artists': self.artists,
            'venues': self.venues,
            'tours': self.tours,
            'events': self.events,
            'event_ratings': self.event_ratings,
            'venue_reviews': self.venue_reviews,
            'artist_ratings': self.artist_ratings,
            'ticket_sales': self.ticket_sales,
            'user_artist_follows': self.user_follows
        }
        
        for name, data in datasets.items():
            if data:
                df = pd.DataFrame(data)
                filepath = os.path.join(self.output_dir, f'{name}.csv')
                df.to_csv(filepath, index=False)
                print(f"  ✓ Saved {len(data):,} records to {name}.csv")
    
    def print_summary_statistics(self):
        """Print summary statistics about generated data"""
        
        print("\n" + "="*60)
        print("GENERATED DATA SUMMARY")
        print("="*60)
        
        # Basic counts
        print(f"\nRecord Counts:")
        print(f"  • Cities: {len(self.cities):,}")
        print(f"  • Users: {len(self.users):,}")
        print(f"  • Artists: {len(self.artists):,}")
        print(f"  • Venues: {len(self.venues):,}")
        print(f"  • Tours: {len(self.tours):,}")
        print(f"  • Events: {len(self.events):,}")
        print(f"  • Event Ratings: {len(self.event_ratings):,}")
        print(f"  • Venue Reviews: {len(self.venue_reviews):,}")
        print(f"  • Artist Ratings: {len(self.artist_ratings):,}")
        print(f"  • Ticket Sales: {len(self.ticket_sales):,}")
        print(f"  • User Follows: {len(self.user_follows):,}")
        
        # User statistics
        power_users = [u for u in self.users if u['user_segment'] == 'power_user']
        verified_users = [u for u in self.users if u['user_type'] == 'verified']
        print(f"\nUser Breakdown:")
        print(f"  • Power Users: {len(power_users):,} ({len(power_users)/len(self.users)*100:.1f}%)")
        print(f"  • Verified Users: {len(verified_users):,} ({len(verified_users)/len(self.users)*100:.1f}%)")
        
        # Event statistics
        completed_events = [e for e in self.events if e['event_status'] == 'completed']
        cancelled_events = [e for e in self.events if e['event_status'] == 'cancelled']
        print(f"\nEvent Status:")
        print(f"  • Completed: {len(completed_events):,}")
        print(f"  • Cancelled: {len(cancelled_events):,}")
        print(f"  • Scheduled: {len(self.events) - len(completed_events) - len(cancelled_events):,}")
        
        # Rating statistics
        if self.event_ratings:
            ratings_df = pd.DataFrame(self.event_ratings)
            avg_rating = ratings_df['rating_score'].mean()
            print(f"\nRating Statistics:")
            print(f"  • Average Rating: {avg_rating:.2f}")
            print(f"  • Total Ratings: {len(self.event_ratings):,}")
            print(f"  • Ratings per Event: {len(self.event_ratings)/len(completed_events):.1f}")
        
        # Data quality issues
        print(f"\nIntentional Data Quality Issues:")
        print(f"  • Duplicate ratings: ~15% of event_ratings")
        print(f"  • Unverified venue capacities: ~20% of venues")
        print(f"  • Missing VIP prices: ~30% of events")
        print(f"  • Temporal anomalies: ~2% ratings before event date")
        print(f"  • Bot attacks: ~1% of events have suspicious rating patterns")
        print(f"  • Name inconsistencies: ~5% of artists have name variations")
        
        print("\n" + "="*60)
        print("Data generation complete! Ready for import to BQ.")
        print("="*60)

# ================================================================================
# UTILITY FUNCTIONS
# ================================================================================

def validate_data_relationships(generator):
    """Validate that all foreign key relationships are valid"""
    
    print("\nValidating data relationships...")
    
    errors = []
    
    # Check event -> artist relationships
    artist_ids = {a['artist_id'] for a in generator.artists}
    for event in generator.events:
        if event['artist_id'] not in artist_ids:
            errors.append(f"Event {event['event_id']} references non-existent artist {event['artist_id']}")
    
    # Check event -> venue relationships
    venue_ids = {v['venue_id'] for v in generator.venues}
    for event in generator.events:
        if event['venue_id'] not in venue_ids:
            errors.append(f"Event {event['event_id']} references non-existent venue {event['venue_id']}")
    
    # Check rating -> event relationships
    event_ids = {e['event_id'] for e in generator.events}
    for rating in generator.event_ratings:
        if rating['event_id'] not in event_ids:
            errors.append(f"Rating {rating['rating_id']} references non-existent event {rating['event_id']}")
    
    # Check rating -> user relationships
    user_ids = {u['user_id'] for u in generator.users}
    for rating in generator.event_ratings:
        if rating['user_id'] not in user_ids:
            errors.append(f"Rating {rating['rating_id']} references non-existent user {rating['user_id']}")
    
    if errors:
        print(f"Found {len(errors)} relationship errors:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"    • {error}")
    else:
        print("All relationships valid!")
    
    return len(errors) == 0

def generate_data_dictionary(generator, output_dir='data'):
    """Generate a data dictionary documenting all tables and columns"""
    
    print("\n📖 Generating data dictionary...")
    
    dictionary = {
        'cities': {
            'description': 'Reference table of cities with music scene metrics',
            'columns': {
                'city_id': 'Unique identifier for city',
                'city': 'City name',
                'state': 'State abbreviation',
                'population': 'City population',
                'music_scene_score': 'Music scene quality (1-10)',
                'primary_genres': 'JSON array of popular genres',
                'avg_ticket_price': 'Average concert ticket price',
                'total_venues': 'Number of music venues',
                'timezone': 'Timezone identifier'
            }
        },
        'users': {
            'description': 'Platform users who rate events and follow artists',
            'columns': {
                'user_id': 'Unique user identifier',
                'username': 'Display username',
                'email': 'User email address',
                'user_type': 'verified or regular',
                'user_segment': 'power_user, regular, or casual',
                'join_date': 'Account creation date',
                'home_city': 'User home city',
                'home_state': 'User home state',
                'age_group': 'Age bracket',
                'preferred_genres': 'JSON array of genre preferences',
                'profile_completeness': 'Profile completion percentage',
                'email_verified': 'Email verification status',
                'push_notifications_enabled': 'Push notification preference',
                'last_active_date': 'Last platform activity'
            }
        },
        'artists': {
            'description': 'Musical artists and bands',
            'columns': {
                'artist_id': 'Unique artist identifier',
                'artist_name': 'Artist or band name',
                'formed_year': 'Year artist/band formed',
                'origin_city': 'Artist origin city',
                'origin_state': 'Artist origin state',
                'origin_country': 'Artist origin country',
                'spotify_monthly_listeners': 'Spotify listener count',
                'instagram_followers': 'Instagram follower count',
                'genre_primary': 'Primary music genre',
                'genre_secondary': 'Secondary music genre',
                'booking_price_min': 'Minimum booking fee',
                'booking_price_max': 'Maximum booking fee',
                'popularity_tier': 'Popularity classification',
                'tour_frequency': 'How often artist tours',
                'average_show_duration_minutes': 'Typical show length',
                'has_label': 'Signed to record label',
                'verified_artist': 'Platform verification status'
            }
        },
        'venues': {
            'description': 'Concert venues and their attributes',
            'columns': {
                'venue_id': 'Unique venue identifier',
                'venue_name': 'Venue name',
                'address': 'Street address',
                'city': 'Venue city',
                'state': 'Venue state',
                'zip_code': 'Postal code',
                'latitude': 'Geographic latitude',
                'longitude': 'Geographic longitude',
                'venue_type': 'Type of venue (club, arena, etc)',
                'capacity': 'Seating capacity',
                'standing_room_capacity': 'Total capacity with standing room',
                'opened_year': 'Year venue opened',
                'parking_available': 'On-site parking availability',
                'valet_parking': 'Valet service available',
                'food_available': 'Food service available',
                'full_bar': 'Full bar service',
                'accessible_ada': 'ADA accessibility',
                'box_office': 'Has box office',
                'typical_ticket_fee': 'Average ticket service fee',
                'venue_website': 'Venue website URL',
                'phone': 'Contact phone number',
                'validated_capacity': 'Capacity verification status'
            }
        },
        'events': {
            'description': 'Individual concert events',
            'columns': {
                'event_id': 'Unique event identifier',
                'event_name': 'Event name/title',
                'artist_id': 'Performing artist (FK)',
                'venue_id': 'Event venue (FK)',
                'tour_id': 'Associated tour (FK, nullable)',
                'event_date': 'Event date',
                'event_day_of_week': 'Day of week',
                'doors_time': 'Doors open time',
                'show_time': 'Show start time',
                'announced_date': 'Date event was announced',
                'on_sale_date': 'Ticket sale start date',
                'base_ticket_price': 'Standard ticket price',
                'vip_ticket_price': 'VIP ticket price (nullable)',
                'ticket_vendor': 'Primary ticket vendor',
                'age_restriction': 'Age requirements',
                'opening_acts': 'JSON array of opening acts',
                'event_status': 'Status (completed, scheduled, cancelled)',
                'cancellation_reason': 'Reason if cancelled',
                'estimated_attendance': 'Attendance count',
                'weather_condition': 'Weather for outdoor venues',
                'special_event': 'Special event flag'
            }
        },
        'event_ratings': {
            'description': 'User ratings and reviews of events',
            'columns': {
                'rating_id': 'Unique rating identifier',
                'event_id': 'Rated event (FK)',
                'user_id': 'Rating user (FK)',
                'rating_score': 'Overall rating (1-5)',
                'rating_date': 'Date of rating',
                'days_after_event': 'Days between event and rating',
                'review_title': 'Review title (nullable)',
                'review_text': 'Review text (nullable)',
                'verified_attendance': 'Attendance verification status',
                'helpful_count': 'Number of helpful votes',
                'reported': 'Flagged as inappropriate',
                'aspects': 'JSON object with detailed ratings'
            }
        }
    }
    
    # Save as markdown
    with open(os.path.join(output_dir, 'data_dictionary.md'), 'w') as f:
        f.write("# Soundcheck Analytics - Data Dictionary\n\n")
        f.write("Generated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
        
        for table_name, table_info in dictionary.items():
            f.write(f"## {table_name}\n\n")
            f.write(f"**Description:** {table_info['description']}\n\n")
            f.write("| Column | Description | \n")
            f.write("|--------|-------------|\n")
            for col_name, col_desc in table_info['columns'].items():
                f.write(f"| `{col_name}` | {col_desc} |\n")
            f.write("\n")
    
    print("Data dictionary saved to data_dictionary.md")

# ================================================================================
# MAIN EXECUTION
# ================================================================================

if __name__ == "__main__":
    print("""
        Soundcheck Analytics - Data Generator
        Generating realistic fake data for live music analytics
    """)
    
    # Initialize generator
    generator = SoundcheckDataGenerator(output_dir='data/raw')
    
    # Generate all data
    generator.generate_all_data()
    
    # Validate relationships
    if validate_data_relationships(generator):
        print("\n All data relationships are valid")
    
    # Generate documentation
    generate_data_dictionary(generator, output_dir='data')
    
    print("""

    Generation Complete!
    
      Next steps:
      1. Review data in data/raw/ directory
      2. Upload CSVs to BigQuery RAW schema
      3. Begin dbt modeling
   
    """)