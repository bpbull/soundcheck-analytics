# Soundcheck Analytics - Data Dictionary

Generated: 2025-09-27 13:37:40

## cities

**Description:** Reference table of cities with music scene metrics

| Column | Description | 
|--------|-------------|
| `city_id` | Unique identifier for city |
| `city` | City name |
| `state` | State abbreviation |
| `population` | City population |
| `music_scene_score` | Music scene quality (1-10) |
| `primary_genres` | JSON array of popular genres |
| `avg_ticket_price` | Average concert ticket price |
| `total_venues` | Number of music venues |
| `timezone` | Timezone identifier |

## users

**Description:** Platform users who rate events and follow artists

| Column | Description | 
|--------|-------------|
| `user_id` | Unique user identifier |
| `username` | Display username |
| `email` | User email address |
| `user_type` | verified or regular |
| `user_segment` | power_user, regular, or casual |
| `join_date` | Account creation date |
| `home_city` | User home city |
| `home_state` | User home state |
| `age_group` | Age bracket |
| `preferred_genres` | JSON array of genre preferences |
| `profile_completeness` | Profile completion percentage |
| `email_verified` | Email verification status |
| `push_notifications_enabled` | Push notification preference |
| `last_active_date` | Last platform activity |

## artists

**Description:** Musical artists and bands

| Column | Description | 
|--------|-------------|
| `artist_id` | Unique artist identifier |
| `artist_name` | Artist or band name |
| `formed_year` | Year artist/band formed |
| `origin_city` | Artist origin city |
| `origin_state` | Artist origin state |
| `origin_country` | Artist origin country |
| `spotify_monthly_listeners` | Spotify listener count |
| `instagram_followers` | Instagram follower count |
| `genre_primary` | Primary music genre |
| `genre_secondary` | Secondary music genre |
| `booking_price_min` | Minimum booking fee |
| `booking_price_max` | Maximum booking fee |
| `popularity_tier` | Popularity classification |
| `tour_frequency` | How often artist tours |
| `average_show_duration_minutes` | Typical show length |
| `has_label` | Signed to record label |
| `verified_artist` | Platform verification status |

## venues

**Description:** Concert venues and their attributes

| Column | Description | 
|--------|-------------|
| `venue_id` | Unique venue identifier |
| `venue_name` | Venue name |
| `address` | Street address |
| `city` | Venue city |
| `state` | Venue state |
| `zip_code` | Postal code |
| `latitude` | Geographic latitude |
| `longitude` | Geographic longitude |
| `venue_type` | Type of venue (club, arena, etc) |
| `capacity` | Seating capacity |
| `standing_room_capacity` | Total capacity with standing room |
| `opened_year` | Year venue opened |
| `parking_available` | On-site parking availability |
| `valet_parking` | Valet service available |
| `food_available` | Food service available |
| `full_bar` | Full bar service |
| `accessible_ada` | ADA accessibility |
| `box_office` | Has box office |
| `typical_ticket_fee` | Average ticket service fee |
| `venue_website` | Venue website URL |
| `phone` | Contact phone number |
| `validated_capacity` | Capacity verification status |

## events

**Description:** Individual concert events

| Column | Description | 
|--------|-------------|
| `event_id` | Unique event identifier |
| `event_name` | Event name/title |
| `artist_id` | Performing artist (FK) |
| `venue_id` | Event venue (FK) |
| `tour_id` | Associated tour (FK, nullable) |
| `event_date` | Event date |
| `event_day_of_week` | Day of week |
| `doors_time` | Doors open time |
| `show_time` | Show start time |
| `announced_date` | Date event was announced |
| `on_sale_date` | Ticket sale start date |
| `base_ticket_price` | Standard ticket price |
| `vip_ticket_price` | VIP ticket price (nullable) |
| `ticket_vendor` | Primary ticket vendor |
| `age_restriction` | Age requirements |
| `opening_acts` | JSON array of opening acts |
| `event_status` | Status (completed, scheduled, cancelled) |
| `cancellation_reason` | Reason if cancelled |
| `estimated_attendance` | Attendance count |
| `weather_condition` | Weather for outdoor venues |
| `special_event` | Special event flag |

## event_ratings

**Description:** User ratings and reviews of events

| Column | Description | 
|--------|-------------|
| `rating_id` | Unique rating identifier |
| `event_id` | Rated event (FK) |
| `user_id` | Rating user (FK) |
| `rating_score` | Overall rating (1-5) |
| `rating_date` | Date of rating |
| `days_after_event` | Days between event and rating |
| `review_title` | Review title (nullable) |
| `review_text` | Review text (nullable) |
| `verified_attendance` | Attendance verification status |
| `helpful_count` | Number of helpful votes |
| `reported` | Flagged as inappropriate |
| `aspects` | JSON object with detailed ratings |