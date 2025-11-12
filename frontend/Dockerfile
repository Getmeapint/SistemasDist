# Usar a imagem base do Nginx
FROM nginx:alpine

# Copiar o arquivo HTML para o diretório padrão do Nginx
COPY . /usr/share/nginx/html/

# Ensure images live under assets/images inside the container for the app to reference
RUN mkdir -p /usr/share/nginx/html/assets/images \
	&& cp /usr/share/nginx/html/male.png /usr/share/nginx/html/assets/images/ 2>/dev/null || true \
	&& cp /usr/share/nginx/html/female.png /usr/share/nginx/html/assets/images/ 2>/dev/null || true \
	&& cp /usr/share/nginx/html/start.png /usr/share/nginx/html/assets/images/ 2>/dev/null || true \
	&& cp /usr/share/nginx/html/finish.png /usr/share/nginx/html/assets/images/ 2>/dev/null || true