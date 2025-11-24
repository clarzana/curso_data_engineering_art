# curso_data_engineering_art
Proyecto propio para curso de Data Engineering, a partir de algunos de los datos en crudo de [PainterPalette](https://github.com/me9hanics/PainterPalette)

<!--
table artist /*formerly painterpalette*/ {
  //++++++++++++++++
  artist_id varchar(32)
  name varchar(512)
  nationality array //fusionar con citizenship
  //~~~~~~~~~~~~~~original: gender varchar(512)
  gender_id varchar(32)
  birth_place_id varchar(512)
  death_place_id varchar(512)
  birth_year varchar(512)
  death_year varchar(512)
  firstyear varchar(512)
  lastyear varchar(512)
  paintingsexhibitedat array
  paintingsexhibitedatcount array
  paintingschool varchar(512)
  contemporary varchar(512)
  artmovement array
  type array
}
//++++++++++++++++
table genders{
  gender_id varchar(32)
  gender_name varchar(512)
}
//++++++++++++++++

//++++++++++++++++
table styles{
  style_id varchar(32)
  style_name varchar(256)
  style_origin_country_id varchar(32)
}
//++++++++++++++++
//++++++++++++++++
table movements{
  movement_id varchar(32)
  movement_name	varchar(256)
  movement_origin_country_id	varchar(32)
}
//++++++++++++++++
table painting_schools{
  painting_school_id varchar(32)
  painting_school_name varchar(512)
  painting_school_origin_country_id varchar(32)
}
//++++++++++++++++
table places {
  place_id varchar(32)
  place_name varchar(256)
}
//++++++++++++++++
//++++++++++++++++
table countries{
  country_id varchar(32)
  country_name varchar(256)
  demonym varchar(256)
}
//++++++++++++++++
//++++++++++++++++
table occupations{
  occupation_id varchar(32)
  occupation_name varchar(256)
}
//++++++++++++++++
//++++++++++++++++
table influences{
  influencer_artist_id varchar(32)
  influenced_artist_id varchar(512)
}
//++++++++++++++++
table mentorships{
  teacher_artist_id varchar(32) //who is the teacher
  pupil_artist_id varchar(32) //who got taught
}
//++++++++++++++++
//++++++++++++++++


//++++++++++++++++
table artworks {
  artwork_id varchar(32) [primary key]
  //Varios artistas pueden ser autores de una obra:
  //crear tablas intermedias para:
  // artista, escuela de arte, influencias(para obras)
  // tags, instituciones de arte
  //Añadir theme como una tag más
  artwork_name varchar(256)
  image_url varchar(512)
  media varchar(1024) //fusionar con columna field de art500k
  date date
  location_id varchar(512)
  height float
  width float
  depth float
  series_id varchar(512)
  //quitar period, friendsandcoworkers, familyandrelatives,
  //teachers, pupils y path
}



table exhibitions_journals{
  exhibition_journal_id	varchar(8)
  eflux_link	varchar(512)
  title	varchar(512)
  subtitle	varchar(512)
  announcement_date	date
}

table media {
  medium_id varchar(32) [primary key]
  medium_description varchar(512)
}

table genres{
  genre_id varchar(32)
  genre_name varchar(512)
}


table artworks_genres{
  artwork_genre_id varchar(32)
  artwork_id varchar(32)
  genre_id varchar(32)
}
table artworks_media{
  artworks_media_id varchar(32)
  medium_id varchar(32)
  artwork_id varchar(32)
}
table citizenships{
  citizenship_id varchar(32) [primary key]
  country_id varchar(32)
  artist_id varchar(32)
}
table styles_artists{
  style_artist_id varchar(32) [primary key]
  artist_id varchar(32)
  style_id varchar(32)
}
table artworks_styles{
  artwork_style_id varchar(32) [primary key]
  style_id varchar(32)
  artwork_id varchar(32)
}
table movements_artists {
  movement_artist_id varchar(32) [primary key]
  movement_id varchar(32)
  artist_id varchar(32)
}
table artworks_movements {
  movement_artwork_id varchar(32) [primary key]
  movement_id varchar(32)
  artwork_id varchar(32)
}
table locations_artists {
  location_artist_id varchar(32)
  location_id varchar(32)
  artist_id varchar(32)
}
table occupations_artists{
  occupation_artist_id varchar(32)
  occupation_id varchar(32)
  artist_id varchar(32)
}
table painting_schools_artists{
  painting_schools_artists_id varchar(32)
  painting_school_id varchar(32)
  artist_id varchar(32)
}
table painting_schools_artworks{
  painting_schools_artwork_id varchar(32)
  painting_school_id varchar(32)
  artwork_id varchar(32)
}
table authorships {
  authorship_id varchar(32)
  artwork_id varchar(32)
  artist_id varchar(32)
}
table exhibitions_journals_artists{
  exhibition_journal_artist_id varchar(32)
  exhibition_journal_id varchar(32)
  artist_id varchar(32)
  in_title boolean
}
Ref: "citizenships"."country_id" < "countries"."country_id"

Ref: "citizenships"."country_id" < "artist"."artist_id"

Ref: "genders"."gender_id" < "artist"."gender_id"

Ref: "styles_artists"."style_artist_id" < "artist"."artist_id"

Ref: "styles_artists"."style_artist_id" < "styles"."style_id"



Ref: "movements_artists"."movement_id" < "movements"."movement_id"

Ref: "movements_artists"."artist_id" < "artist"."artist_id"

Ref: "places"."place_id" < "artist"."birth_place_id"

Ref: "places"."place_id" < "artist"."death_place_id"

Ref: "locations_artists"."artist_id" < "artist"."artist_id"

Ref: "locations_artists"."location_id" < "places"."place_id"

Ref: "occupations_artists"."occupation_id" < "occupations"."occupation_id"

Ref: "occupations_artists"."artist_id" < "artist"."artist_id"

Ref: "painting_schools_artists"."painting_school_id" < "painting_schools"."painting_school_id"

Ref: "painting_schools_artists"."artist_id" < "artist"."artist_id"

Ref: "influences"."influenced_artist_id" < "artist"."artist_id"

Ref: "influences"."influencer_artist_id" < "artist"."artist_id"

Ref: "mentorships"."teacher_artist_id" < "artist"."artist_id"

Ref: "mentorships"."pupil_artist_id" < "artist"."artist_id"

Ref: "authorships"."artwork_id" < "artworks"."artwork_id"

Ref: "authorships"."artist_id" < "artist"."artist_id"

Ref: "artworks_styles"."artwork_id" < "artworks"."artwork_id"

Ref: "artworks_styles"."style_id" < "styles"."style_id"

Ref: "artworks_movements"."movement_id" < "movements"."movement_id"

Ref: "artworks_movements"."artwork_id" < "artworks"."artwork_name"

Ref: "painting_schools_artworks"."artwork_id" < "artworks"."artwork_id"

Ref: "painting_schools_artworks"."painting_school_id" < "painting_schools"."painting_school_id"

Ref: "exhibitions_journals_artists"."artist_id" < "artist"."artist_id"

Ref: "exhibitions_journals_artists"."exhibition_journal_id" < "exhibitions_journals"."exhibition_journal_id"


Ref: "artworks_media"."medium_id" < "media"."medium_id"

Ref: "artworks_media"."artwork_id" < "artworks"."artwork_id"

Ref: "artworks_genres"."genre_id" < "genres"."genre_id"

Ref: "artworks_genres"."artwork_id" < "artworks"."artwork_id"
 -->