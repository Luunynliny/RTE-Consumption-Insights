<div id="top"></div>

<!-- PROJECT SHIELDS -->

[![python](https://badges.aleen42.com/src/python.svg)](https://www.python.org/)

<!-- PROJECT LOGO -->

<br />

<div align="center">
    <a href="https://github.com/Luunynliny/RTE-Consumption-Insights">
        <img src="imgs/logo-rte-800x800.png" alt="Logo" height="80">
    </a>
    <h3 align="center">RTE Consumption Insights</h3>
    <p align="center">
        Monitoring of France's power consumption and production
    </p>
</div>

<!-- ABOUT THE PROJECT -->

## About

This project goal is to provide a cloud-based solution for the retrieving and visualization of daily data regarding France energy generation and consumption.
Using 3 API endpoints from the Réseau de Transport d'Énergie :
- `actual_generation_per_production_type resource`
- `weekly_forecast`
- `consolidated_power_consumption`

we were able to provide meaningful insights on France energy production distribution, consumption forecast and many more.

### Architecture

<p align="center">
  <img src="imgs/architecture.png">
</p>

### Insights

#### Power production

<p align="center">
  <img src="imgs/pp_distribution.png">
</p>

<p align="center">
  <img src="imgs/pp_timeline.png">
</p>

<p align="center">
  <img src="imgs/pp_month.png">
</p>

#### Power consumption

<p align="center">
  <img src="imgs/pc_timeline.png">
</p>

<p align="center">
  <img src="imgs/pc_month.png">
</p>

#### Forecasting

<p align="center">
  <img src="imgs/f_timeline.png">
</p>

<p align="center">
  <img src="imgs/f_days_difference.png">
</p>

<p align="center">
  <img src="imgs/f_difference.png">
</p>

<p align="right"><a href="#top"><i>back to top</i></a></p>

## License

Distributed under the Creative Commons Zero v1.0 Universal License. See `LICENSE`for more information.

<p align="right"><a href="#top"><i>back to top</i></a></p>

<!-- RESOURCES -->

## Resources

RTE. (n.d.-a). Actual Generation. Catalogue API - API data RTE. https://data.rte-france.com/catalog/-/api/generation/Actual-Generation/v1.1

RTE. (n.d.-b). Consolidated Consumption. Catalogue API - API data RTE. https://data.rte-france.com/catalog/-/api/consumption/Consolidated-Consumption/v1.0

RTE. (n.d.-c). Consumption. Catalogue API - API data RTE. https://data.rte-france.com/catalog/-/api/consumption/Consumption/v1.2 

<p align="right"><a href="#top"><i>back to top</i></a></p>