library(RPostgreSQL)
library(curl)
library(sf)	    # simple feature
library(sp)		# Spatial (for e.g. SpatialPointsDataframe)
library(raster)
library(gstat)  # The script uses gstat's idw routine
library(rgdal)
library(jsonlite)

university <- list("ru", "bursa", "ugent")

depth <- c(300,300,300)        # The considered root depth [mm]
MAD_Setting <- c(0.6,0.6,0.6)  # The set Maximum allowable depletion [%]
fill_up_rate <- c(0.9,0.9,0.9) # Irrigate until X % of field capacity

for (i in 1:2) {
	print(university[i])
    ###################################################################
    ## Download the Field Mask from GeoNode
    h <- new_handle()
    handle_setopt(h, VERBOSE = 0)         #libcurl doc: https://curl.se/libcurl/c/
    handle_setopt(h, SSL_VERIFYPEER = 0)  # for insecure connections
    filename <- paste("field_boundaries_",university[i], sep = "")
    URL1 <- "https://geoportal.addferti.eu/geoserver/ows"
    URL2 <- "?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3A"
    URL4 <- filename
    URL5 <- "&outputFormat=json"
    URL  <- paste(URL1,URL2,URL4,URL5,sep="")
    print(URL)
    # Download the file.
    curl_download(URL, handle = h, destfile = paste(filename,".geojson", sep= ""))

    # read the data using sf (simple feature = dataframe with geometry)
    sf.Field.Mask <- st_read(paste(filename,".geojson", sep= ""),quiet = TRUE)
	print(paste("EPSG of",filename,": ",st_crs(sf.Field.Mask)$epsg))

    ###################################################################
    ## Download the Field Capacity and PWP from GeoNode
    h <- new_handle()
    handle_setopt(h, VERBOSE = 0)         #libcurl doc: https://curl.se/libcurl/c/
    handle_setopt(h, SSL_VERIFYPEER = 0)  # for insecure connections
    filename <- paste("fc_pwp_",university[i], sep = "")
    URL1 <- "https://geoportal.addferti.eu/geoserver/ows"
    URL2 <- "?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3A"
    URL4 <- filename
    URL5 <- "&outputFormat=json"
    URL  <- paste(URL1,URL2,URL4,URL5,sep="")
    print(URL)
    # Download the file.
    curl_download(URL, handle = h, destfile = paste(filename,".geojson", sep= ""))

    # read the data using sf (simple feature = dataframe with geometry)
    sf.fc <- st_read(paste(filename,".geojson", sep= ""),quiet = TRUE)
	print(sf.fc)
	# Transform the sf into a SpatialPointsDataFrame:
	fc.spdf <- as(sf.fc, "Spatial")
	# Reproject the crs of the fc.spdf to match the Field Mask
    fc.spdf <-  spTransform(fc.spdf, crs(sf.Field.Mask))
	
    ###################################################################
    ## Download the Soil Moisture Data from the database
    #Tables = ['ru_soil_moisture','bursa_soil_moisture','ugent_soil_moisture']
    postgreSQLTable = paste(university[i],"_soil_moisture",sep="")
    dsn_database    = "postgres"    # Postgres databasename
    dsn_hostname    = "127.0.0.1"  
    dsn_port        = "5432"                
    dsn_uid         = "postgres"            # Postgres username 
    dsn_pwd         = "postgres"            # Postgres password 
    tryCatch({
        drv <- dbDriver("PostgreSQL")
        print(paste("Reading from Database ",postgreSQLTable))
        connec <- dbConnect(drv, 
                     dbname = dsn_database,
                     host = dsn_hostname, 
                     port = dsn_port,
                     user = dsn_uid, 
                     password = dsn_pwd)
        },
        error=function(cond) {
                print(paste("Unable to connect to Database",university[i]))
        })
    
	## Select the most current soil moisture data
    SQL1 <- "SELECT DISTINCT ON (device_id) device_id, soil_temp, soil_mc, soil_ec, lat, long FROM"
    SQL2 <- postgreSQLTable
    SQL3 <- "ORDER BY device_id, time desc; " 
    SQL <- paste(SQL1, SQL2, SQL3)
    sensor.df <- dbGetQuery(connec, SQL)
    print(sensor.df)
	
	###################################################################
    ## Download the Rain Forecast data from the database
    #WeatherTables = ['ru_weather','bursa_weather','ugent_weather']
	postgreSQLTable = paste(university[i],"_weather",sep="")
    ## Select the most current soil moisture data
    SQL1 <- "SELECT SUM(rain) FROM (SELECT rain FROM"
    SQL2 <- postgreSQLTable
    SQL3 <- "ORDER BY date DESC LIMIT 1) subquery; " 
    SQL <- paste(SQL1, SQL2, SQL3)
	print(SQL)
    rain <- dbGetQuery(connec, SQL)
	rain[is.na(rain)] <- 0  # replace NA with 0 values
	rain <- rain[ , ]  # convert dataframe to numeric
    print(paste(university[i], " rain: ", rain))
    
    ###################################################################
    ## Interpolate the Measured Soil Moisture Content [%]
    # Make a SpatialPointsDataFrame
    data        <- sensor.df[ , c("device_id", "soil_temp", "soil_mc", "soil_ec")]
    coords      <- sensor.df[ , c("long", "lat")]
    crs         <- CRS("+init=epsg:4326") # => [+proj=longlat +datum=WGS84]
    sensor.spdf <- SpatialPointsDataFrame(coords      = coords,
                                          data        = data, 
                                          proj4string = crs)
    # Reproject the crs of the sensor.spdf to match the Field Mask
    sensor.spdf <- spTransform(sensor.spdf, crs(sf.Field.Mask))
    # Create an empty grid using the extends of the Field Mask with Pixel Size 5 meter
    bbox <- st_bbox(sf.Field.Mask)
    cell_size <- 5
    x <- seq(bbox$xmin, bbox$xmax, by=cell_size)
    y <- seq(bbox$ymin, bbox$ymax, by=cell_size)
    grd <- expand.grid(x=x, y=y)
    names(grd)       <- c("X", "Y")
    coordinates(grd) <- c("X", "Y")
    gridded(grd)     <- TRUE  # Create SpatialPixel object
    fullgrid(grd)    <- TRUE  # Create SpatialGrid object
    # Add P's projection information to the empty grid
    proj4string(grd) <- proj4string(sensor.spdf)
    
    # IDW: Interpolate using a power value of 2 (idp=2.0)
    sensor.idw <- gstat::idw(soil_mc ~ 1, sensor.spdf, newdata=grd, idp=2.0)

    # Convert to raster object then clip to field extend
	# vmc = volumetric moisture content [%]
    vmc.raster.idw <- raster(sensor.idw)

    vmc.raster.idw <- mask(vmc.raster.idw,sf.Field.Mask)
    ###################################################################	
	## Calculations

	## Calculate FC_mm and PWP_mm
	# FC [mm]  =  FC [%] * VW [g/cm3] * depth [mm] / 100
	fc.spdf$FC_mm  <-  fc.spdf$FC * fc.spdf$VW_g_cm3 / 100 * depth[i]

	# PWP [mm] = PWP [%] * VW [g/cm3] * depth [mm] / 100
	fc.spdf$PWP_mm <-  fc.spdf$PWP * fc.spdf$VW_g_cm3 / 100 * depth[i]

    ## Interpolate the Field Capacity
    # IDW: Interpolate using a power value of 2 (idp=2.0)
    fc.spdf.idw <- gstat::idw(FC_mm ~ 1, fc.spdf, newdata=grd, idp=2.0)
    
    # Convert to raster object then clip to field extend
    fc.raster.idw <- raster(fc.spdf.idw)
    fc.raster.idw <- mask(fc.raster.idw,sf.Field.Mask)
    
    ## Interpolate the PWP
    # IDW: Interpolate using a power value of 2 (idp=2.0)
    pwp.spdf.idw <- gstat::idw(PWP_mm ~ 1, fc.spdf, newdata=grd, idp=2.0)
    
    # Convert to raster object then clip to field extend
    pwp.raster.idw <- raster(pwp.spdf.idw)
    pwp.raster.idw <- mask(pwp.raster.idw,sf.Field.Mask)
		
    ## Calculate the Irrigation need
    #  vmc.raster.idw :   MC  [%]  : Volumetric Moisture Content [%]
    #   mc.raster.idw :   MC  [mm] : Calculed Moisture Content for 300 mm root depth [mm]
    #   fc.raster.idw :   FC  [mm] : Field Capacity [mm]
    #  pwp.raster.idw :   PWP [mm] : Field Capacity [mm]
    #   aw.raster.idw :   AW  [mm] : Available Water [mm]
    #   in.raster.idw :   IN  [mm] : Irrigation Need [mm]
    
    # mc[mm] = vmc[%] * 300/100
    mc.raster.idw <- vmc.raster.idw / 100 * depth[i]
    
    # IN [mm] = FC [mm] * 0.9 - MC [mm]
    in.raster.idw <- fc.raster.idw * fill_up_rate[i] - mc.raster.idw - rain
	
	# AW [mm] = FC [mm] - PWP [mm]
	aw.raster.idw <- fc.raster.idw - pwp.raster.idw
	
	# MAD [mm] = AW [mm] * MAD [%] + PWP [mm]
	mad.raster.idw <- aw.raster.idw * MAD_Setting[i] + pwp.raster.idw
	
	# Water left until MAD = MC [mm] - MAD [mm]
	wl.raster.idw = mc.raster.idw - mad.raster.idw
    
    ###################################################################
    # Plots
    pal1 <- colorRampPalette(c("white", "blue"))
    pal2 <- colorRampPalette(c("white", "brown"))
    pal3 <- colorRampPalette(c("white", "darkblue"))
	pal4 <- colorRampPalette(c("red", "blue"))
	par(mfrow=c(2,2)) #Multiplot 2x2 Grid
	plot(vmc.raster.idw, col = pal1(n=7), main = paste(university[i],"Soil moisture content [%]"))
    plot(sensor.spdf, add=TRUE)
    plot(mc.raster.idw, col = pal1(n=7), main = paste(university[i],"Soil moisture content [mm]"))
    plot(sensor.spdf, add=TRUE)
    plot(fc.raster.idw, col = pal2(n=7), main=paste(university[i],"Field Capacity [mm]"))
    plot(fc.spdf, add=TRUE)
    plot(pwp.raster.idw, col = pal2(n=7), main=paste(university[i],"Permanent Wilting Point [mm]"))
    plot(fc.spdf, add=TRUE)
    plot(in.raster.idw, main = paste(university[i],"Irrigation Need [mm]"), col = pal3(n=7))
    plot(wl.raster.idw, main = paste(university[i],"Water left until MAD [mm]"), col = pal4(n=7))

    ###################################################################
    # Save the Application Map as a GeoTiff
	folder <- paste("VRI_",university[i],"_application_map/",sep="")
    filename <- paste("VRI_",university[i],"_application_map",sep="")
	pathAndName <- paste(folder ,filename,sep="")
	       
	## Export the Irrigation Map Shapefile
    # Create a new directory if it does not exist
    isExist <- file.exists(folder)
    if (!isExist) {
      dir.create(folder)
    }
	in.spdf <- rasterToPolygons(in.raster.idw)
	raster::shapefile(in.spdf, pathAndName, overwrite=TRUE)
    #writeRaster(in.raster.idw, filename, format = "GTiff", overwrite = TRUE)
	
	## Export "Water left until MAD" as a matrix:
	wl.raster.idw <- aggregate(wl.raster.idw, fact=4) # aggregate fom 5x5 m to 
	# Reproject to WGS84 + longlat
	wl.raster.idw <- projectRaster(wl.raster.idw, crs="+proj=longlat +datum=WGS84")
	# Transform Raster to Point matrix with format: "x, y, value"
	wl.matrix.idw <- rasterToPoints(wl.raster.idw, spatial=FALSE)
	colnames(wl.matrix.idw) <- c("x", "y", "wl")
	filename <- paste("water_left_",university[i],".txt",sep="")
	write.table(wl.matrix.idw, file = filename, sep = ",", row.names = FALSE, col.names = TRUE)
}  
