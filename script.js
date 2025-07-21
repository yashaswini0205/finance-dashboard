document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.querySelector('.search-bar input');
    const searchButton = document.getElementById('search-button');
    const suggestionsContainer = document.querySelector('.suggestions');

    searchInput.addEventListener('input', () => {
        const query = searchInput.value.trim();
        if (query) {
            fetchCompanies(query);
        } else {
            suggestionsContainer.innerHTML = '';
        }
    });

    searchInput.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            const query = searchInput.value.trim();
            if (query) {
                fetchCompanies(query);
            } else {
                suggestionsContainer.innerHTML = '';
            }
        }
    });

    async function fetchCompanies(query) {
        try {
            const response = await fetch(`/search?q=${encodeURIComponent(query)}`);
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const companies = await response.json();

            // Alphabetical sort
            companies.sort((a, b) => a.name.localeCompare(b.name));

            displaySuggestions(companies);
        } catch (error) {
            console.error('Error fetching companies:', error);
        }
    }

    function displaySuggestions(companies) {
        suggestionsContainer.innerHTML = '';
        companies.forEach((company, index) => {
            setTimeout(() => {
                const button = document.createElement('button');
                button.textContent = company.name;
                button.classList.add('suggestion-item');
                button.addEventListener('click', () => {
                    navigateToDashboard(company.name);
                });
                suggestionsContainer.appendChild(button);
            }, index * 200); // fade-in delay
        });
    }

    function navigateToDashboard(companyName) {
        window.location.href = `/dashboard?company=${encodeURIComponent(companyName)}`;
    }
});
