BLOCK_RESOURCES_TYPES=[
        'beacon',
        'csp_report',
        'font',
        'image',
        'imageset'
        'media',
        'object',
        'texttrack'
]

BLOCK_RESOURCE_NAMES=[
    'adzerk',
    'analytics',
    'cdn.api.twtter',
    'doubleclick',
    'exelator',
    'facebook',
    'fontawesome',
    'google',
    'google-analytics',
    'googletagmanager'
]

async def intercept_route(route):
    if route.request.resource_type in BLOCK_RESOURCES_TYPES:
        return await route.abort()
    if any(key in route.request.url for key in BLOCK_RESOURCE_NAMES):
        return await route.abort()
    return await route.continue_()

